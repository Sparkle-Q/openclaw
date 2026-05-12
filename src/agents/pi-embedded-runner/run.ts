import { randomBytes } from "node:crypto";
import fs from "node:fs/promises";
import type { ReplyPayload } from "../../auto-reply/reply-payload.js";
import type { ThinkLevel } from "../../auto-reply/thinking.js";
import { SILENT_REPLY_TOKEN } from "../../auto-reply/tokens.js";
import { ensureContextEnginesInitialized } from "../../context-engine/init.js";
import {
  resolveContextEngine,
  resolveContextEngineOwnerPluginId,
} from "../../context-engine/registry.js";
import { emitAgentPlanEvent } from "../../infra/agent-events.js";
import { sleepWithAbort } from "../../infra/backoff.js";
import { freezeDiagnosticTraceContext } from "../../infra/diagnostic-trace-context.js";
import { formatErrorMessage } from "../../infra/errors.js";
import { buildAgentHookContextChannelFields } from "../../plugins/hook-agent-context.js";
import { getGlobalHookRunner } from "../../plugins/hook-runner-global.js";
import { resolveProviderAuthProfileId } from "../../plugins/provider-runtime.js";
import { enqueueCommandInLane } from "../../process/command-queue.js";
import type { CommandQueueEnqueueOptions } from "../../process/command-queue.types.js";
import { normalizeOptionalString } from "../../shared/string-coerce.js";
import { sanitizeForLog } from "../../terminal/ansi.js";
import { resolveUserPath } from "../../utils.js";
import { isMarkdownCapableMessageChannel } from "../../utils/message-channel.js";
import {
  hasConfiguredModelFallbacks,
  resolveAgentExecutionContract,
  resolveAgentDir,
  resolveSessionAgentIds,
  resolveAgentWorkspaceDir,
} from "../agent-scope.js";
import {
  type AuthProfileFailureReason,
  type AuthProfileStore,
  isProfileInCooldown,
  markAuthProfileFailure,
  markAuthProfileSuccess,
  resolveAuthProfileEligibility,
} from "../auth-profiles.js";
import { listActiveProcessSessionReferences } from "../bash-process-references.js";
import {
  resolveSessionKeyForRequest,
  resolveStoredSessionKeyForSessionId,
} from "../command/session.js";
import { DEFAULT_MODEL, DEFAULT_PROVIDER } from "../defaults.js";
import { isStrictAgenticExecutionContractActive } from "../execution-contract.js";
import {
  coerceToFailoverError,
  describeFailoverError,
  FailoverError,
  resolveFailoverStatus,
} from "../failover-error.js";
import { ensureSelectedAgentHarnessPlugin } from "../harness/runtime-plugin.js";
import { selectAgentHarness } from "../harness/selection.js";
import { LiveSessionModelSwitchError } from "../live-model-switch-error.js";
import { shouldSwitchToLiveModel, clearLiveModelSwitchPending } from "../live-model-switch.js";
import {
  applyAuthHeaderOverride,
  applyLocalNoAuthHeaderOverride,
  ensureAuthProfileStoreWithoutExternalProfiles,
  type ResolvedProviderAuth,
  resolveAuthProfileOrder,
  shouldPreferExplicitConfigApiKeyAuth,
} from "../model-auth.js";
import { ensureOpenClawModelsJson } from "../models-config.js";
import {
  retireSessionMcpRuntime,
  retireSessionMcpRuntimeForSessionKey,
} from "../pi-bundle-mcp-tools.js";
import {
  classifyFailoverReason,
  extractObservedOverflowTokenCount,
  type FailoverReason,
  formatAssistantErrorText,
  isAuthAssistantError,
  isBillingAssistantError,
  isCompactionFailureError,
  isFailoverAssistantError,
  isFailoverErrorMessage,
  isLikelyContextOverflowError,
  isRateLimitAssistantError,
  parseImageDimensionError,
  parseImageSizeError,
  pickFallbackThinkingLevel,
} from "../pi-embedded-helpers.js";
import { resolveProcessToolScopeKey } from "../pi-tools.js";
import { resolveProviderIdForAuth } from "../provider-auth-aliases.js";
import { runAgentCleanupStep } from "../run-cleanup-timeout.js";
import { buildAgentRuntimeAuthPlan } from "../runtime-plan/auth.js";
import { buildAgentRuntimePlan } from "../runtime-plan/build.js";
import { ensureRuntimePluginsLoaded } from "../runtime-plugins.js";
import { resolveSessionSuspensionReason, suspendSession } from "../session-suspension.js";
import { resolveToolLoopDetectionConfig } from "../tool-loop-detection-config.js";
import { derivePromptTokens, normalizeUsage, type UsageLike } from "../usage.js";
import { redactRunIdentifier, resolveRunWorkspaceDir } from "../workspace-run.js";
import { runPostCompactionSideEffects } from "./compaction-hooks.js";
import { buildEmbeddedCompactionRuntimeContext } from "./compaction-runtime-context.js";
import { resolveContextEngineCapabilities } from "./context-engine-capabilities.js";
import { runContextEngineMaintenance } from "./context-engine-maintenance.js";
import { hasMessagingToolDeliveryEvidence } from "./delivery-evidence.js";
import { resolveEmbeddedRunFailureSignal } from "./failure-signal.js";
import { resolveGlobalLane, resolveSessionLane } from "./lanes.js";
import { log } from "./logger.js";
import { resolveModelAsync } from "./model.js";
import {
  createPostCompactionLoopGuard,
  PostCompactionLoopPersistedError,
  type PostCompactionGuardObservation,
} from "./post-compaction-loop-guard.js";
import { createEmbeddedRunReplayState, observeReplayMetadata } from "./replay-state.js";
import { handleAssistantFailover } from "./run/assistant-failover.js";
import {
  createEmbeddedRunStageTracker,
  formatEmbeddedRunStageSummary,
  shouldWarnEmbeddedRunStageSummary,
} from "./run/attempt-stage-timing.js";
import { forgetPromptBuildDrainCacheForRun } from "./run/attempt.prompt-helpers.js";
import { createEmbeddedRunAuthController } from "./run/auth-controller.js";
import { resolveAuthProfileFailureReason } from "./run/auth-profile-failure-policy.js";
import { runEmbeddedAttemptWithBackend } from "./run/backend.js";
import { createFailoverDecisionLogger } from "./run/failover-observation.js";
import { mergeRetryFailoverReason, resolveRunFailoverDecision } from "./run/failover-policy.js";
import {
  buildErrorAgentMeta,
  buildUsageAgentMetaFields,
  createCompactionDiagId,
  resolveActiveErrorContext,
  resolveFinalAssistantRawText,
  resolveFinalAssistantVisibleText,
  resolveMaxRunRetryIterations,
  resolveReportedModelRef,
  resolveOverloadFailoverBackoffMs,
  resolveOverloadProfileRotationLimit,
  resolveRateLimitProfileRotationLimit,
  type RuntimeAuthState,
  scrubAnthropicRefusalMagic,
} from "./run/helpers.js";
import {
  MAX_CONSECUTIVE_IDLE_TIMEOUTS_BEFORE_OUTPUT,
  createIdleTimeoutBreakerState,
  stepIdleTimeoutBreaker,
} from "./run/idle-timeout-breaker.js";
import {
  DEFAULT_EMPTY_RESPONSE_RETRY_LIMIT,
  DEFAULT_REASONING_ONLY_RETRY_LIMIT,
  resolveAckExecutionFastPathInstruction,
  resolveAttemptReplayMetadata,
  extractPlanningOnlyPlanDetails,
  resolveEmptyResponseRetryInstruction,
  resolveIncompleteTurnPayloadText,
  resolvePlanningOnlyRetryLimit,
  resolvePlanningOnlyRetryInstruction,
  resolveReasoningOnlyRetryInstruction,
  resolveSilentToolResultReplyPayload,
  STRICT_AGENTIC_BLOCKED_TEXT,
  resolveReplayInvalidFlag,
  resolveRunLivenessState,
  shouldTreatEmptyAssistantReplyAsSilent,
} from "./run/incomplete-turn.js";
import type { RunEmbeddedPiAgentParams } from "./run/params.js";
import { buildEmbeddedRunPayloads } from "./run/payloads.js";
import { handleRetryLimitExhaustion } from "./run/retry-limit.js";
import {
  buildBeforeModelResolveAttachments,
  resolveEffectiveRuntimeModel,
  resolveHookModelSelection,
} from "./run/setup.js";
import { mergeAttemptToolMediaPayloads } from "./run/tool-media-payloads.js";
import {
  resolveLiveToolResultMaxChars,
  sessionLikelyHasOversizedToolResults,
  truncateOversizedToolResultsInSession,
} from "./tool-result-truncation.js";
import type {
  EmbeddedPiAgentMeta,
  EmbeddedPiRunResult,
  TraceAttempt,
  ToolSummaryTrace,
  EmbeddedRunLivenessState,
} from "./types.js";
import { createUsageAccumulator, mergeUsageIntoAccumulator } from "./usage-accumulator.js";

type ApiKeyInfo = ResolvedProviderAuth;  //已经处理好的AI服务商认证信息

const MAX_SAME_MODEL_IDLE_TIMEOUT_RETRIES = 1;  //AI 模型调用超时 / 空闲断开时，最多自动重试 1 次
const EMBEDDED_RUN_LANE_TIMEOUT_GRACE_MS = 30_000;  //AI 对话在后台嵌入式执行时，给 30 秒缓冲时间，避免立即判定超时
const MID_TURN_PRECHECK_CONTINUATION_PROMPT =
  "Continue from the current transcript after the latest tool result. Do not repeat the original user request, and do not rerun completed tools unless the transcript shows they are still needed.";
const COMPACTION_CONTINUATION_RETRY_INSTRUCTION =
  "The previous attempt compacted the conversation context before producing a final user-visible answer. Continue from the compacted transcript and produce the final answer now. Do not restart from scratch, do not repeat completed work, and do not rerun tools unless the transcript clearly lacks required evidence.";

//给 AI 运行结果定义一个类型名称，让后续代码能安全使用这个结果的字段。
type EmbeddedRunAttemptForRunner = Awaited<ReturnType<typeof runEmbeddedAttemptWithBackend>>;

//根据传入的 provider 和 harnessId，返回最终要使用的 provider 字符串
function resolveHarnessContextConfigProvider(params: {
  provider: string;
  harnessId: string;
}): string {
  if (params.harnessId === "codex" && params.provider.trim().toLowerCase() === "openai") {
    return "openai-codex";
  }
  return params.provider;
}

//计算嵌入式运行通道的**最终超时时间**
//给原始超时时间增加 30 秒宽限期，并做合法性校验；无效时间直接返回 undefined。
function resolveEmbeddedRunLaneTimeoutMs(timeoutMs: number): number | undefined {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    return undefined;
  }
  return Math.floor(timeoutMs) + EMBEDDED_RUN_LANE_TIMEOUT_GRACE_MS;
}

//给队列任务设置“嵌入式运行通道超时时间”
//只有【有超时 + 原配置没超时】 → 才把超时设置进去
function withEmbeddedRunLaneTimeout(
  opts: CommandQueueEnqueueOptions | undefined,
  laneTaskTimeoutMs: number | undefined,
): CommandQueueEnqueueOptions | undefined {
  if (laneTaskTimeoutMs === undefined || opts?.taskTimeoutMs !== undefined) {
    return opts;
  }
  return { ...opts, taskTimeoutMs: laneTaskTimeoutMs };
}

//把 AI 嵌入式运行的结果“标准化”
//确保所有数组/对象字段一定存在，不会是 null / undefined，防止代码报错
function normalizeEmbeddedRunAttemptResult(
  attempt: EmbeddedRunAttemptForRunner,
): EmbeddedRunAttemptForRunner {
  
  // 第一步：把原始数据转成“允许字段为 null”的类型
  // 因为后端/数据库可能返回 null，但前端/业务期望永远是数组/对象
  const raw = attempt as EmbeddedRunAttemptForRunner & {
    assistantTexts?: EmbeddedRunAttemptForRunner["assistantTexts"] | null;
    toolMetas?: EmbeddedRunAttemptForRunner["toolMetas"] | null;
    messagesSnapshot?: EmbeddedRunAttemptForRunner["messagesSnapshot"] | null;
    messagingToolSentTexts?: EmbeddedRunAttemptForRunner["messagingToolSentTexts"] | null;
    messagingToolSentMediaUrls?: EmbeddedRunAttemptForRunner["messagingToolSentMediaUrls"] | null;
    messagingToolSentTargets?: EmbeddedRunAttemptForRunner["messagingToolSentTargets"] | null;
    itemLifecycle?: EmbeddedRunAttemptForRunner["itemLifecycle"] | null;
  };
  
  // 第二步：返回一个**全新的、安全的对象**
  return {
    ...attempt,  // 保留所有原有字段

    // 核心：空值安全替换（null / undefined → 替换成默认值）
    assistantTexts: raw.assistantTexts ?? [],
    toolMetas: raw.toolMetas ?? [],
    messagesSnapshot: raw.messagesSnapshot ?? [],
    messagingToolSentTexts: raw.messagingToolSentTexts ?? [],
    messagingToolSentMediaUrls: raw.messagingToolSentMediaUrls ?? [],
    messagingToolSentTargets: raw.messagingToolSentTargets ?? [],
    
    // 对象类型：空 → 给默认结构（全0）
    itemLifecycle: raw.itemLifecycle ?? {
      startedCount: 0,
      completedCount: 0,
      activeCount: 0,
    },
    // 额外处理：解析重试/回放元数据
    replayMetadata: resolveAttemptReplayMetadata(raw),
  };
}

//判断 AI 运行任务是否已经产生了 “有效进度”，用于防止任务无限空转 / 超时中断（idle breaker）
function hasCompletedModelProgressForIdleBreaker(attempt: EmbeddedRunAttemptForRunner): boolean {
  return (
    // 1. 有非空的助手回复文本
    attempt.assistantTexts.some((text) => text.trim().length > 0) ||
    // 2. 调用过工具
    attempt.toolMetas.length > 0 ||
    // 3. 有客户端工具调用
    (attempt.clientToolCalls?.length ?? 0) > 0 ||
    // 4. 有消息工具发送证据（发过消息/文件）
    hasMessagingToolDeliveryEvidence(attempt) ||
    // 5. 有任务完成计数
    attempt.itemLifecycle.completedCount > 0
  );
}

//创建一个空的、初始状态的认证配置存储器，用于存储用户的 API Key、认证信息等。
function createEmptyAuthProfileStore(): AuthProfileStore {
  return {
    version: 1,
    profiles: {},
  };
}

/**
 * 从完整的认证配置仓库中，筛选出指定 profileIds 的配置
 * 创建一个【作用域限定】的、仅包含指定凭证的新配置仓库
 */
function createScopedAuthProfileStore(
  store: AuthProfileStore,  // 完整的认证配置仓库（所有密钥/配置）
  profileIds: string | undefined | string[],  // 要保留的配置ID（单个/数组/空）
): AuthProfileStore {            // 返回：筛选后的新配置仓库
  
  // 1. 安全获取所有凭证，空值兜底为 {}
  const profiles = store.profiles ?? {};
  // 2. 把传入的 profileIds 统一格式化为【干净的字符串数组】
  const normalizedProfileIds = (Array.isArray(profileIds) ? profileIds : [profileIds])
    .map((profileId) => profileId?.trim())
    .filter((profileId): profileId is string => !!profileId);
  // 3. 从完整配置中，只挑出【存在且有效】的指定ID凭证
  const scopedProfiles = Object.fromEntries(
    normalizedProfileIds.flatMap((profileId) => {
      const credential = profiles[profileId];  // 查找对应ID的凭证
      return credential ? [[profileId, credential] as const] : [];  // 有就保留，没有就丢弃
    }),
  );
  
  // 4. 最终返回：有筛选结果 → 返回；无结果 → 返回空初始配置
  return Object.keys(scopedProfiles).length > 0
    ? {
        version: store.version,   // 保留原版本号
        profiles: scopedProfiles,  // 只保留筛选后的凭证
      }
    : createEmptyAuthProfileStore();   // 空 → 创建空认证仓库
}

/**
 * 构建工具调用追踪摘要
 * 作用：从工具元数据中，统计调用次数、不重复的工具列表、是否失败
 */
function buildTraceToolSummary(params: {
  toolMetas?: Array<{ toolName: string; meta?: string }>;  // 工具调用记录
  hadFailure: boolean;   // 是否有失败
}): ToolSummaryTrace | undefined {   // 返回摘要 或 undefined（无工具时）
  
  // 1. 如果没有工具调用记录 → 直接返回 undefined，不生成摘要
  if (!params.toolMetas?.length) {
    return undefined;
  }
  // 2. 初始化：工具名称数组 + 去重 Set
  const tools: string[] = [];
  const seen = new Set<string>();

  // 3. 遍历所有工具调用，**去重**收集工具名称
  for (const entry of params.toolMetas) {
    // 标准化工具名（空值处理、trim 等）
    const toolName = normalizeOptionalString(entry.toolName);
    // 如果工具名不存在，或已经记录过 → 跳过
    if (!toolName || seen.has(toolName)) {
      continue;
    }
    // 否则 → 加入去重集合和结果列表
    seen.add(toolName);
    tools.push(toolName);
  }
  // 4. 返回最终的工具调用摘要
  return {
    calls: params.toolMetas?.length ?? 0,  // 总调用次数
    tools,   // 不重复的工具名称列表
    failures: params.hadFailure ? 1 : 0,   // 失败标记（0=无，1=有）
  };
}

/**
 * Best-effort backfill of sessionKey from sessionId when not explicitly provided.
 * The return value is normalized: whitespace-only inputs collapse to undefined, and
 * successful resolution returns a trimmed session key. This is a read-only lookup
 * with no side effects.
 * See: https://github.com/openclaw/openclaw/issues/60552
 */
// 会话密钥自动回填函数，属于 AI 代理会话管理的核心工具。
// 输入sessionId，自动找到/补全sessionKey
function backfillSessionKey(params: {
  config: RunEmbeddedPiAgentParams["config"];  // 系统配置
  sessionId: string;     // 会话 ID（必须）
  sessionKey?: string;   // 会话密钥（可选，要回填的就是它）
  agentId?: string;      // 代理 ID（可选）
}): string | undefined {   // 返回：sessionKey 或 undefined
  // 1. 先把传入的 sessionKey 标准化（去空格、空值转 undefined）
  const trimmed = normalizeOptionalString(params.sessionKey);
  // 2. 如果用户已经提供了有效的 sessionKey → 直接返回，不做任何事
  if (trimmed) {
    return trimmed;
  }
  // 3. 如果没有 config 或没有 sessionId → 无法回填，返回 undefined
  if (!params.config || !params.sessionId) {
    return undefined;
  }
  try {
    // 4. 根据是否有 agentId，调用不同的方法去【通过 sessionId 查找 sessionKey】
    const resolved = normalizeOptionalString(params.agentId)
      ? resolveStoredSessionKeyForSessionId({  // 有 agentId → 走存储查询
          cfg: params.config,
          sessionId: params.sessionId,
          agentId: params.agentId,
        })
      : resolveSessionKeyForRequest({     // 无 agentId → 走请求查询
          cfg: params.config,
          sessionId: params.sessionId,
        });
    // 5. 返回查询到的 sessionKey（再次标准化）
    return normalizeOptionalString(resolved.sessionKey);
  } catch (err) {
    // 6. 异常处理：查询失败 → 打警告日志，返回 undefined
    log.warn(
      `[backfillSessionKey] Failed to resolve sessionKey for sessionId=${redactRunIdentifier(sanitizeForLog(params.sessionId))}: ${formatErrorMessage(err)}`,
    );
    return undefined;
  }
}

// AI 回复内容标准化函数
// 把传入的回复统一包装成固定格式的数组，确保程序永远不会报错。
function buildHandledReplyPayloads(reply?: ReplyPayload) {
  // 1. 标准化：如果 reply 是 null/undefined → 自动替换为【静默回复】
  const normalized = reply ?? { text: SILENT_REPLY_TOKEN };
  return [
    {
      text: normalized.text,            // 回复文本
      mediaUrl: normalized.mediaUrl,    // 单个媒体地址
      mediaUrls: normalized.mediaUrls,  // 多个媒体地址
      replyToId: normalized.replyToId,  // 回复目标ID
      audioAsVoice: normalized.audioAsVoice,  // 音频是否作为语音
      isError: normalized.isError,       // 是否是错误消息
      isReasoning: normalized.isReasoning,  // 是否是思考过程
    },
  ];
}

export async function runEmbeddedPiAgent(
  params: RunEmbeddedPiAgentParams,
): Promise<EmbeddedPiRunResult> {
  // Resolve sessionKey early so all downstream consumers (hooks, LCM, compaction)
  // receive a non-null key even when callers omit it. See #60552.
  const effectiveSessionKey = backfillSessionKey({
    config: params.config,
    sessionId: params.sessionId,
    sessionKey: params.sessionKey,
    agentId: params.agentId,
  });
  if (effectiveSessionKey !== params.sessionKey) {
    params = { ...params, sessionKey: effectiveSessionKey };
  }
  const sessionLane = resolveSessionLane(params.sessionKey?.trim() || params.sessionId);
  const globalLane = resolveGlobalLane(params.lane);
  const laneTaskTimeoutMs = resolveEmbeddedRunLaneTimeoutMs(params.timeoutMs);
  const withLaneTimeout = (opts?: CommandQueueEnqueueOptions) =>
    withEmbeddedRunLaneTimeout(opts, laneTaskTimeoutMs);
  const enqueueGlobal = <T>(task: () => Promise<T>, opts?: CommandQueueEnqueueOptions) =>
    params.enqueue
      ? params.enqueue(task, withLaneTimeout(opts))
      : enqueueCommandInLane(globalLane, task, withLaneTimeout(opts));
  const enqueueSession = <T>(task: () => Promise<T>, opts?: CommandQueueEnqueueOptions) =>
    params.enqueue ? params.enqueue(task, opts) : enqueueCommandInLane(sessionLane, task, opts);
  const channelHint = params.messageChannel ?? params.messageProvider;
  const resolvedToolResultFormat =
    params.toolResultFormat ??
    (channelHint
      ? isMarkdownCapableMessageChannel(channelHint)
        ? "markdown"
        : "plain"
      : "markdown");
  const isProbeSession = params.sessionId?.startsWith("probe-") ?? false;

  const throwIfAborted = () => {
    if (!params.abortSignal?.aborted) {
      return;
    }
    const reason = params.abortSignal.reason;
    if (reason instanceof Error) {
      throw reason;
    }
    const abortErr =
      reason !== undefined
        ? new Error("Operation aborted", { cause: reason })
        : new Error("Operation aborted");
    abortErr.name = "AbortError";
    throw abortErr;
  };

  throwIfAborted();

  return enqueueSession(() => {
    throwIfAborted();
    return enqueueGlobal(async () => {
      throwIfAborted();
      const started = Date.now();
      const startupStages = createEmbeddedRunStageTracker();
      let startupStagesEmitted = false;
      const notifyExecutionPhase = (
        phase: Parameters<NonNullable<RunEmbeddedPiAgentParams["onExecutionPhase"]>>[0]["phase"],
        extra?: Omit<
          Parameters<NonNullable<RunEmbeddedPiAgentParams["onExecutionPhase"]>>[0],
          "phase"
        >,
      ) => {
        params.onExecutionPhase?.({ phase, ...extra });
      };
      const emitStartupStageSummary = (phase: string) => {
        const summary = startupStages.snapshot();
        const shouldWarn = shouldWarnEmbeddedRunStageSummary(summary);
        if (!shouldWarn && !log.isEnabled("trace")) {
          return;
        }
        const message = formatEmbeddedRunStageSummary(
          `[trace:embedded-run] startup stages: runId=${params.runId} sessionId=${params.sessionId} phase=${phase}`,
          summary,
        );
        if (shouldWarn) {
          log.warn(message);
        } else {
          log.trace(message);
        }
      };
      params.onExecutionStarted?.();
      notifyExecutionPhase("runner_entered");
      const workspaceResolution = resolveRunWorkspaceDir({
        workspaceDir: params.workspaceDir,
        sessionKey: params.sessionKey,
        agentId: params.agentId,
        config: params.config,
      });
      const resolvedWorkspace = workspaceResolution.workspaceDir;
      const canonicalWorkspace = resolveUserPath(
        resolveAgentWorkspaceDir(params.config ?? {}, workspaceResolution.agentId),
      );
      const isCanonicalWorkspace = canonicalWorkspace === resolvedWorkspace;
      const redactedSessionId = redactRunIdentifier(params.sessionId);
      const redactedSessionKey = redactRunIdentifier(params.sessionKey);
      const redactedWorkspace = redactRunIdentifier(resolvedWorkspace);
      if (workspaceResolution.usedFallback) {
        log.warn(
          `[workspace-fallback] caller=runEmbeddedPiAgent reason=${workspaceResolution.fallbackReason} run=${params.runId} session=${redactedSessionId} sessionKey=${redactedSessionKey} agent=${workspaceResolution.agentId} workspace=${redactedWorkspace}`,
        );
      }
      startupStages.mark("workspace");
      notifyExecutionPhase("workspace");
      ensureRuntimePluginsLoaded({
        config: params.config,
        workspaceDir: resolvedWorkspace,
        allowGatewaySubagentBinding: params.allowGatewaySubagentBinding,
      });
      startupStages.mark("runtime-plugins");
      notifyExecutionPhase("runtime_plugins");

      let provider = (params.provider ?? DEFAULT_PROVIDER).trim() || DEFAULT_PROVIDER;
      let modelId = (params.model ?? DEFAULT_MODEL).trim() || DEFAULT_MODEL;
      const agentDir =
        params.agentDir ?? resolveAgentDir(params.config ?? {}, workspaceResolution.agentId);
      const normalizedSessionKey = params.sessionKey?.trim();
      const fallbackConfigured = hasConfiguredModelFallbacks({
        cfg: params.config,
        agentId: params.agentId,
        sessionKey: normalizedSessionKey,
      });
      const resolvedSessionKey = normalizedSessionKey;
      const hookRunner = getGlobalHookRunner();
      const hookCtx = {
        runId: params.runId,
        jobId: params.jobId,
        agentId: workspaceResolution.agentId,
        sessionKey: resolvedSessionKey,
        sessionId: params.sessionId,
        workspaceDir: resolvedWorkspace,
        modelProviderId: provider,
        modelId,
        trigger: params.trigger,
        ...buildAgentHookContextChannelFields(params),
      };
      if (params.trigger === "cron" && hookRunner?.hasHooks("before_agent_reply")) {
        const hookResult = await hookRunner.runBeforeAgentReply(
          { cleanedBody: params.prompt },
          hookCtx,
        );
        if (hookResult?.handled) {
          return {
            payloads: buildHandledReplyPayloads(hookResult.reply),
            meta: {
              durationMs: Date.now() - started,
              agentMeta: {
                sessionId: params.sessionId,
                provider,
                model: modelId,
              },
              finalAssistantVisibleText: hookResult.reply?.text ?? SILENT_REPLY_TOKEN,
              finalAssistantRawText: hookResult.reply?.text ?? SILENT_REPLY_TOKEN,
            },
          };
        }
      }

      const hookSelection = await resolveHookModelSelection({
        prompt: params.prompt,
        attachments: buildBeforeModelResolveAttachments(params.images),
        provider,
        modelId,
        hookRunner,
        hookContext: hookCtx,
      });
      provider = hookSelection.provider;
      modelId = hookSelection.modelId;
      const legacyBeforeAgentStartResult = hookSelection.legacyBeforeAgentStartResult;
      startupStages.mark("hooks");
      await ensureSelectedAgentHarnessPlugin({
        provider,
        modelId,
        config: params.config,
        agentId: params.agentId,
        sessionKey: params.sessionKey,
        workspaceDir: resolvedWorkspace,
      });
      const agentHarness = selectAgentHarness({
        provider,
        modelId,
        config: params.config,
        agentId: params.agentId,
        sessionKey: params.sessionKey,
        agentHarnessId: params.agentHarnessId,
      });
      const pluginHarnessOwnsTransport = agentHarness.id !== "pi";
      const dynamicModelResolution = await resolveModelAsync(
        provider,
        modelId,
        agentDir,
        params.config,
        {
          // Plugin dynamic model hooks can resolve explicit model refs without
          // first generating PI models.json. This keeps one-shot model runs from
          // blocking on unrelated provider discovery.
          skipPiDiscovery: true,
          workspaceDir: resolvedWorkspace,
        },
      );
      const modelResolution =
        dynamicModelResolution.model || pluginHarnessOwnsTransport
          ? dynamicModelResolution
          : await (async () => {
              await ensureOpenClawModelsJson(params.config, agentDir, {
                workspaceDir: resolvedWorkspace,
              });
              return await resolveModelAsync(provider, modelId, agentDir, params.config, {
                workspaceDir: resolvedWorkspace,
              });
            })();
      const { model, error, authStorage, modelRegistry } = modelResolution;
      if (!model) {
        throw new FailoverError(error ?? `Unknown model: ${provider}/${modelId}`, {
          reason: "model_not_found",
          provider,
          model: modelId,
          sessionId: params.sessionId,
          lane: globalLane,
        });
      }
      let runtimeModel = model;

      const resolvedRuntimeModel = resolveEffectiveRuntimeModel({
        cfg: params.config,
        provider,
        contextConfigProvider: resolveHarnessContextConfigProvider({
          provider,
          harnessId: agentHarness.id,
        }),
        modelId,
        runtimeModel,
      });
      const ctxInfo = resolvedRuntimeModel.ctxInfo;
      let effectiveModel = resolvedRuntimeModel.effectiveModel;
      startupStages.mark("model-resolution");
      notifyExecutionPhase("model_resolution", { provider, model: modelId });

      const authStore = pluginHarnessOwnsTransport
        ? createEmptyAuthProfileStore()
        : ensureAuthProfileStoreWithoutExternalProfiles(agentDir, {
            allowKeychainPrompt: false,
          });
      const attemptAuthProfileStore = pluginHarnessOwnsTransport
        ? ensureAuthProfileStoreWithoutExternalProfiles(agentDir, {
            allowKeychainPrompt: false,
          })
        : authStore;
      const requestedProfileId = params.authProfileId?.trim();
      const requestedProfileIsUserLocked = params.authProfileIdSource === "user";
      const isForwardablePluginHarnessAuthProfile = (
        profileId: string | undefined,
      ): profileId is string => {
        if (!pluginHarnessOwnsTransport || !profileId) {
          return false;
        }
        const credential = attemptAuthProfileStore.profiles?.[profileId];
        const runtimeAuthPlan = buildAgentRuntimeAuthPlan({
          provider,
          authProfileProvider: credential?.provider ?? profileId.split(":", 1)[0],
          authProfileMode: credential?.type,
          sessionAuthProfileId: profileId,
          config: params.config,
          workspaceDir: resolvedWorkspace,
          harnessId: agentHarness.id,
          harnessRuntime: agentHarness.id,
          allowHarnessAuthProfileForwarding: true,
        });
        return runtimeAuthPlan.forwardedAuthProfileId === profileId;
      };
      const resolvePluginHarnessProfileOrder = (): string[] => {
        if (requestedProfileId && requestedProfileIsUserLocked) {
          return isForwardablePluginHarnessAuthProfile(requestedProfileId)
            ? [requestedProfileId]
            : [];
        }
        if (!pluginHarnessOwnsTransport) {
          return [];
        }
        const runtimeAuthPlan = buildAgentRuntimeAuthPlan({
          provider,
          config: params.config,
          workspaceDir: resolvedWorkspace,
          harnessId: agentHarness.id,
          harnessRuntime: agentHarness.id,
          allowHarnessAuthProfileForwarding: true,
        });
        const harnessAuthProvider = runtimeAuthPlan.harnessAuthProvider;
        if (!harnessAuthProvider) {
          return [];
        }
        const resolvedOrder = resolveAuthProfileOrder({
          cfg: params.config,
          store: attemptAuthProfileStore,
          provider: harnessAuthProvider,
        }).filter(isForwardablePluginHarnessAuthProfile);
        if (resolvedOrder.length > 0) {
          return resolvedOrder;
        }
        if (requestedProfileId && isForwardablePluginHarnessAuthProfile(requestedProfileId)) {
          return [requestedProfileId];
        }
        return [];
      };
      const pluginHarnessProfileOrder = pluginHarnessOwnsTransport
        ? resolvePluginHarnessProfileOrder()
        : [];
      const resolvePluginHarnessPreferredProfileId = (): string | undefined =>
        pluginHarnessProfileOrder[0];
      const preferredProfileId = pluginHarnessOwnsTransport
        ? resolvePluginHarnessPreferredProfileId()
        : requestedProfileId;
      let lockedProfileId = requestedProfileIsUserLocked ? preferredProfileId : undefined;
      if (lockedProfileId) {
        if (pluginHarnessOwnsTransport) {
          if (!isForwardablePluginHarnessAuthProfile(lockedProfileId)) {
            lockedProfileId = undefined;
          }
        } else {
          const lockedProfile = authStore.profiles[lockedProfileId];
          const lockedProfileProvider = lockedProfile
            ? resolveProviderIdForAuth(lockedProfile.provider, {
                config: params.config,
                workspaceDir: resolvedWorkspace,
              })
            : undefined;
          const runProvider = resolveProviderIdForAuth(provider, {
            config: params.config,
            workspaceDir: resolvedWorkspace,
          });
          if (!lockedProfile || !lockedProfileProvider || lockedProfileProvider !== runProvider) {
            lockedProfileId = undefined;
          }
        }
      }
      const forwardedPluginHarnessProfileId =
        pluginHarnessOwnsTransport &&
        !lockedProfileId &&
        isForwardablePluginHarnessAuthProfile(preferredProfileId)
          ? preferredProfileId
          : undefined;
      if (lockedProfileId && !pluginHarnessOwnsTransport) {
        const eligibility = resolveAuthProfileEligibility({
          cfg: params.config,
          store: authStore,
          provider,
          profileId: lockedProfileId,
        });
        if (!eligibility.eligible) {
          throw new Error(`Auth profile "${lockedProfileId}" is not configured for ${provider}.`);
        }
      }
      const profileOrder = shouldPreferExplicitConfigApiKeyAuth(params.config, provider)
        ? []
        : resolveAuthProfileOrder({
            cfg: params.config,
            store: authStore,
            provider,
            preferredProfile: preferredProfileId,
          });
      const providerPreferredProfileId = lockedProfileId
        ? undefined
        : resolveProviderAuthProfileId({
            provider,
            config: params.config,
            workspaceDir: resolvedWorkspace,
            context: {
              config: params.config,
              agentDir,
              workspaceDir: resolvedWorkspace,
              provider,
              modelId,
              preferredProfileId,
              lockedProfileId,
              profileOrder,
              authStore,
            },
          });
      const providerOrderedProfiles =
        providerPreferredProfileId && profileOrder.includes(providerPreferredProfileId)
          ? [
              providerPreferredProfileId,
              ...profileOrder.filter((profileId) => profileId !== providerPreferredProfileId),
            ]
          : profileOrder;
      const profileCandidates = pluginHarnessOwnsTransport
        ? lockedProfileId
          ? [lockedProfileId]
          : pluginHarnessProfileOrder.length > 0
            ? pluginHarnessProfileOrder
            : [undefined]
        : lockedProfileId
          ? [lockedProfileId]
          : providerOrderedProfiles.length > 0
            ? providerOrderedProfiles
            : [undefined];
      const pluginHarnessForwardedProfileCandidates = pluginHarnessOwnsTransport
        ? profileCandidates.filter(isForwardablePluginHarnessAuthProfile)
        : [];
      const profileFailureStore = pluginHarnessOwnsTransport ? attemptAuthProfileStore : authStore;
      let profileIndex = 0;
      const traceAttempts: TraceAttempt[] = [];

      const initialThinkLevel = params.thinkLevel ?? "off";
      let thinkLevel = initialThinkLevel;
      const attemptedThinking = new Set<ThinkLevel>();
      let apiKeyInfo: ApiKeyInfo | null = null;
      let lastProfileId: string | undefined;
      let runtimeAuthState: RuntimeAuthState | null = null;
      let runtimeAuthRefreshCancelled = false;
      const {
        advanceAuthProfile,
        initializeAuthProfile,
        maybeRefreshRuntimeAuthForAuthError,
        stopRuntimeAuthRefreshTimer,
      } = createEmbeddedRunAuthController({
        config: params.config,
        agentDir,
        workspaceDir: resolvedWorkspace,
        authStore,
        authStorage,
        profileCandidates,
        lockedProfileId,
        initialThinkLevel,
        attemptedThinking,
        fallbackConfigured,
        allowTransientCooldownProbe: params.allowTransientCooldownProbe === true,
        getProvider: () => provider,
        getModelId: () => modelId,
        getRuntimeModel: () => runtimeModel,
        setRuntimeModel: (next) => {
          runtimeModel = next;
        },
        getEffectiveModel: () => effectiveModel,
        setEffectiveModel: (next) => {
          effectiveModel = next;
        },
        getApiKeyInfo: () => apiKeyInfo,
        setApiKeyInfo: (next) => {
          apiKeyInfo = next;
        },
        getLastProfileId: () => lastProfileId,
        setLastProfileId: (next) => {
          lastProfileId = next;
        },
        getRuntimeAuthState: () => runtimeAuthState,
        setRuntimeAuthState: (next) => {
          runtimeAuthState = next;
        },
        getRuntimeAuthRefreshCancelled: () => runtimeAuthRefreshCancelled,
        setRuntimeAuthRefreshCancelled: (next) => {
          runtimeAuthRefreshCancelled = next;
        },
        getProfileIndex: () => profileIndex,
        setProfileIndex: (next) => {
          profileIndex = next;
        },
        setThinkLevel: (next) => {
          thinkLevel = next;
        },
        log,
      });
      const advancePluginHarnessAuthProfile = async (): Promise<boolean> => {
        if (!pluginHarnessOwnsTransport || lockedProfileId) {
          return false;
        }
        let nextIndex = profileIndex + 1;
        while (nextIndex < profileCandidates.length) {
          const candidate = profileCandidates[nextIndex];
          if (!candidate || !isForwardablePluginHarnessAuthProfile(candidate)) {
            nextIndex += 1;
            continue;
          }
          if (isProfileInCooldown(attemptAuthProfileStore, candidate, undefined, modelId)) {
            nextIndex += 1;
            continue;
          }
          profileIndex = nextIndex;
          lastProfileId = candidate;
          thinkLevel = initialThinkLevel;
          attemptedThinking.clear();
          return true;
        }
        return false;
      };

      // Plugin harnesses own their model transport/auth. Running PI's generic
      // auth bootstrap here can turn synthetic provider markers into real
      // vendor-token refresh attempts before the plugin gets control.
      if (!pluginHarnessOwnsTransport) {
        await initializeAuthProfile();
      } else if (lockedProfileId) {
        lastProfileId = lockedProfileId;
      } else if (forwardedPluginHarnessProfileId) {
        lastProfileId = forwardedPluginHarnessProfileId;
      }
      startupStages.mark("auth");
      notifyExecutionPhase("auth", { provider, model: modelId });
      const runAttemptAuthProfileStore = pluginHarnessOwnsTransport
        ? createScopedAuthProfileStore(
            attemptAuthProfileStore,
            pluginHarnessForwardedProfileCandidates.length > 0
              ? pluginHarnessForwardedProfileCandidates
              : lastProfileId,
          )
        : attemptAuthProfileStore;
      const { sessionAgentId } = resolveSessionAgentIds({
        sessionKey: params.sessionKey,
        config: params.config,
        agentId: params.agentId,
      });
      const configuredExecutionContract =
        resolveAgentExecutionContract(params.config, sessionAgentId) ?? "default";
      const strictAgenticActive = isStrictAgenticExecutionContractActive({
        config: params.config,
        sessionKey: params.sessionKey,
        agentId: params.agentId,
        provider,
        modelId,
      });
      const executionContract = strictAgenticActive ? "strict-agentic" : "default";
      const maxPlanningOnlyRetryAttempts = resolvePlanningOnlyRetryLimit(executionContract);
      const maxReasoningOnlyRetryAttempts = DEFAULT_REASONING_ONLY_RETRY_LIMIT;
      const maxEmptyResponseRetryAttempts = DEFAULT_EMPTY_RESPONSE_RETRY_LIMIT;

      const MAX_TIMEOUT_COMPACTION_ATTEMPTS = 2;
      const MAX_OVERFLOW_COMPACTION_ATTEMPTS = 3;
      const MAX_RUN_LOOP_ITERATIONS = resolveMaxRunRetryIterations(profileCandidates.length);
      let overflowCompactionAttempts = 0;
      let toolResultTruncationAttempted = false;
      let bootstrapPromptWarningSignaturesSeen =
        params.bootstrapPromptWarningSignaturesSeen ??
        (params.bootstrapPromptWarningSignature ? [params.bootstrapPromptWarningSignature] : []);
      const usageAccumulator = createUsageAccumulator();
      let lastRunPromptUsage: ReturnType<typeof normalizeUsage> | undefined;
      let autoCompactionCount = 0;
      let lastCompactionTokensAfter: number | undefined;
      let runLoopIterations = 0;
      let overloadProfileRotations = 0;
      let planningOnlyRetryAttempts = 0;
      let reasoningOnlyRetryAttempts = 0;
      let emptyResponseRetryAttempts = 0;
      let compactionContinuationRetryAttempts = 0;
      let sameModelIdleTimeoutRetries = 0;
      // Cost-runaway breaker for #76293. State lives at the run-loop level
      // on purpose so it survives across attempt boundaries and across
      // profile/auth retries within this embedded run (a wrapper-local
      // counter would reset on every iteration). The helper is pure and
      // unit-tested in run/idle-timeout-breaker.test.ts; the run loop just
      // feeds it the outcome of each attempt.
      const idleTimeoutBreakerState = createIdleTimeoutBreakerState();
      // Post-compaction loop guard for #77474. Armed at each compaction-success
      // site below; observed from the live tool-outcome path so it can abort
      // while the post-compaction prompt is still running.
      const resolvedLoopDetectionConfig = resolveToolLoopDetectionConfig({
        cfg: params.config,
        agentId: sessionAgentId,
      });
      const postCompactionGuard = createPostCompactionLoopGuard(
        resolvedLoopDetectionConfig?.postCompactionGuard,
        { enabled: resolvedLoopDetectionConfig?.enabled !== false },
      );
      let postCompactionAbortController: AbortController | undefined;
      let postCompactionAbortError: PostCompactionLoopPersistedError | undefined;
      const observePostCompactionToolOutcome = (
        observation: PostCompactionGuardObservation,
      ): void => {
        const verdict = postCompactionGuard.observe(observation);
        if (verdict.shouldAbort) {
          postCompactionAbortError ??= PostCompactionLoopPersistedError.fromVerdict(verdict);
          postCompactionAbortController?.abort(postCompactionAbortError);
        }
      };
      let lastRetryFailoverReason: FailoverReason | null = null;
      let planningOnlyRetryInstruction: string | null = null;
      let reasoningOnlyRetryInstruction: string | null = null;
      let emptyResponseRetryInstruction: string | null = null;
      let compactionContinuationRetryInstruction: string | null = null;
      let nextAttemptPromptOverride: string | null = null;
      const ackExecutionFastPathInstruction = resolveAckExecutionFastPathInstruction({
        provider,
        modelId,
        prompt: params.prompt,
      });
      let rateLimitProfileRotations = 0;
      let timeoutCompactionAttempts = 0;
      // Silent-error retry: non-strict-agentic models (e.g. ollama/glm-5.1) can
      // end a turn with stopReason="error" + zero output tokens, producing no
      // user-visible text. This is an orthogonal, model-agnostic resubmission
      // for errored turns; stopReason="stop" empty zero-token turns use the
      // visible-answer retry instruction instead.
      const MAX_EMPTY_ERROR_RETRIES = 3;
      let emptyErrorRetries = 0;
      const overloadFailoverBackoffMs = resolveOverloadFailoverBackoffMs(params.config);
      const overloadProfileRotationLimit = resolveOverloadProfileRotationLimit(params.config);
      const rateLimitProfileRotationLimit = resolveRateLimitProfileRotationLimit(params.config);
      let activeSessionId = params.sessionId;
      let activeSessionFile = params.sessionFile;
      let suppressNextUserMessagePersistence = params.suppressNextUserMessagePersistence ?? false;
      // Pi owns JSONL persistence; this marker only lets the outer retry avoid
      // replaying the same inbound channel message after overflow compaction.
      let lastPersistedCurrentMessageId: string | number | undefined;
      const onUserMessagePersisted: RunEmbeddedPiAgentParams["onUserMessagePersisted"] = (
        message,
      ) => {
        if (params.currentMessageId !== undefined) {
          lastPersistedCurrentMessageId = params.currentMessageId;
        }
        params.onUserMessagePersisted?.(message);
      };
      const continueFromCurrentTranscript = () => {
        nextAttemptPromptOverride = MID_TURN_PRECHECK_CONTINUATION_PROMPT;
        suppressNextUserMessagePersistence = true;
      };
      const maybeEscalateRateLimitProfileFallback = (params: {
        failoverProvider: string;
        failoverModel: string;
        logFallbackDecision: (decision: "fallback_model", extra?: { status?: number }) => void;
      }) => {
        rateLimitProfileRotations += 1;
        if (rateLimitProfileRotations <= rateLimitProfileRotationLimit || !fallbackConfigured) {
          return;
        }
        const status = resolveFailoverStatus("rate_limit");
        log.warn(
          `rate-limit profile rotation cap reached for ${sanitizeForLog(provider)}/${sanitizeForLog(modelId)} after ${rateLimitProfileRotations} rotations; escalating to model fallback`,
        );
        params.logFallbackDecision("fallback_model", { status });
        throw new FailoverError(
          "The AI service is temporarily rate-limited. Please try again in a moment.",
          {
            reason: "rate_limit",
            provider: params.failoverProvider,
            model: params.failoverModel,
            profileId: lastProfileId,
            sessionId: activeSessionId,
            lane: globalLane,
            status,
          },
        );
      };
      const maybeMarkAuthProfileFailure = async (failure: {
        profileId?: string;
        reason?: AuthProfileFailureReason | null;
        config?: RunEmbeddedPiAgentParams["config"];
        agentDir?: RunEmbeddedPiAgentParams["agentDir"];
        modelId?: string;
      }) => {
        const { profileId, reason } = failure;
        if (!profileId || !reason || reason === "timeout") {
          return;
        }
        await markAuthProfileFailure({
          store: profileFailureStore,
          profileId,
          reason,
          cfg: params.config,
          agentDir,
          runId: params.runId,
          modelId: failure.modelId,
        });
      };
      const resolveRunAuthProfileFailureReason = (failoverReason: FailoverReason | null) =>
        resolveAuthProfileFailureReason({
          failoverReason,
          policy: params.authProfileFailurePolicy,
        });
      const maybeBackoffBeforeOverloadFailover = async (reason: FailoverReason | null) => {
        if (reason !== "overloaded" || overloadFailoverBackoffMs <= 0) {
          return;
        }
        log.warn(
          `overload backoff before failover for ${provider}/${modelId}: delayMs=${overloadFailoverBackoffMs}`,
        );
        try {
          await sleepWithAbort(overloadFailoverBackoffMs, params.abortSignal);
        } catch (err) {
          if (params.abortSignal?.aborted) {
            const abortErr = new Error("Operation aborted", { cause: err });
            abortErr.name = "AbortError";
            throw abortErr;
          }
          throw err;
        }
      };
      // Resolve the context engine once and reuse across retries to avoid
      // repeated initialization/connection overhead per attempt.
      ensureContextEnginesInitialized();
      const contextEngine = await resolveContextEngine(params.config, {
        agentDir,
        workspaceDir: resolvedWorkspace,
      });
      const contextEnginePluginId = resolveContextEngineOwnerPluginId(contextEngine);
      startupStages.mark("context-engine");
      notifyExecutionPhase("context_engine", { provider, model: modelId });
      try {
        const resolveActiveHookContext = () => ({
          ...hookCtx,
          sessionId: activeSessionId,
        });
        const adoptCompactionTranscript = (
          compactResult: Awaited<ReturnType<typeof contextEngine.compact>>,
        ) => {
          const nextSessionId = compactResult.result?.sessionId;
          const nextSessionFile = compactResult.result?.sessionFile;
          if (nextSessionId && nextSessionId !== activeSessionId) {
            activeSessionId = nextSessionId;
          }
          if (nextSessionFile && nextSessionFile !== activeSessionFile) {
            activeSessionFile = nextSessionFile;
          }
        };
        const onCompactionHookMessages = async (payload: {
          phase: "before" | "after";
          messages: string[];
        }) => {
          const messages = payload.messages.filter((message) => message.trim().length > 0);
          if (messages.length === 0) {
            return;
          }
          await params.onAgentEvent?.({
            stream: "compaction",
            data: {
              phase: payload.phase === "before" ? "start" : "end",
              ...(payload.phase === "after" ? { completed: true } : {}),
              messages,
            },
            ...(params.sessionKey ? { sessionKey: params.sessionKey } : {}),
          });
        };
        // When the engine owns compaction, compactEmbeddedPiSessionDirect is
        // bypassed. Fire lifecycle hooks here so recovery paths still notify
        // subscribers like memory extensions and usage trackers.
        const runOwnsCompactionBeforeHook = async (reason: string) => {
          if (
            contextEngine.info.ownsCompaction !== true ||
            !hookRunner?.hasHooks("before_compaction")
          ) {
            return;
          }
          try {
            await hookRunner.runBeforeCompaction(
              { messageCount: -1, sessionFile: activeSessionFile },
              resolveActiveHookContext(),
            );
          } catch (hookErr) {
            log.warn(`before_compaction hook failed during ${reason}: ${String(hookErr)}`);
          }
        };
        const runOwnsCompactionAfterHook = async (
          reason: string,
          compactResult: Awaited<ReturnType<typeof contextEngine.compact>>,
        ) => {
          if (
            contextEngine.info.ownsCompaction !== true ||
            !compactResult.ok ||
            !compactResult.compacted ||
            !hookRunner?.hasHooks("after_compaction")
          ) {
            return;
          }
          try {
            await hookRunner.runAfterCompaction(
              {
                messageCount: -1,
                compactedCount: -1,
                tokenCount: compactResult.result?.tokensAfter,
                sessionFile: compactResult.result?.sessionFile ?? activeSessionFile,
              },
              resolveActiveHookContext(),
            );
          } catch (hookErr) {
            log.warn(`after_compaction hook failed during ${reason}: ${String(hookErr)}`);
          }
        };
        let authRetryPending = false;
        let accumulatedReplayState = createEmbeddedRunReplayState();
        // Hoisted so the retry-limit error path can use the most recent API total.
        let lastTurnTotal: number | undefined;
        while (true) {
          if (runLoopIterations >= MAX_RUN_LOOP_ITERATIONS) {
            const message =
              `Exceeded retry limit after ${runLoopIterations} attempts ` +
              `(max=${MAX_RUN_LOOP_ITERATIONS}).`;
            log.error(
              `[run-retry-limit] sessionKey=${params.sessionKey ?? params.sessionId} ` +
                `provider=${provider}/${modelId} attempts=${runLoopIterations} ` +
                `maxAttempts=${MAX_RUN_LOOP_ITERATIONS}`,
            );
            const retryLimitDecision = resolveRunFailoverDecision({
              stage: "retry_limit",
              fallbackConfigured,
              failoverReason: lastRetryFailoverReason,
            });
            return handleRetryLimitExhaustion({
              message,
              decision: retryLimitDecision,
              provider,
              model: modelId,
              profileId: lastProfileId,
              durationMs: Date.now() - started,
              agentMeta: buildErrorAgentMeta({
                sessionId: activeSessionId,
                provider,
                model: model.id,
                contextTokens: ctxInfo.tokens,
                usageAccumulator,
                lastRunPromptUsage,
                lastTurnTotal,
              }),
              replayInvalid: accumulatedReplayState.replayInvalid ? true : undefined,
              livenessState: "blocked",
            });
          }
          runLoopIterations += 1;
          const runtimeAuthRetry = authRetryPending;
          authRetryPending = false;
          attemptedThinking.add(thinkLevel);
          await fs.mkdir(resolvedWorkspace, { recursive: true });

          const basePrompt =
            nextAttemptPromptOverride ??
            (provider === "anthropic" ? scrubAnthropicRefusalMagic(params.prompt) : params.prompt);
          nextAttemptPromptOverride = null;
          const promptAdditions = [
            ackExecutionFastPathInstruction,
            planningOnlyRetryInstruction,
            reasoningOnlyRetryInstruction,
            emptyResponseRetryInstruction,
            compactionContinuationRetryInstruction,
          ].filter(
            (value): value is string => typeof value === "string" && value.trim().length > 0,
          );
          const prompt =
            promptAdditions.length > 0
              ? `${basePrompt}\n\n${promptAdditions.join("\n\n")}`
              : basePrompt;
          let resolvedStreamApiKey: string | undefined;
          if (!runtimeAuthState && apiKeyInfo) {
            resolvedStreamApiKey = (apiKeyInfo as ApiKeyInfo).apiKey;
          }
          const runtimePlan = buildAgentRuntimePlan({
            provider,
            modelId,
            model: effectiveModel,
            modelApi: effectiveModel.api,
            harnessId: agentHarness.id,
            harnessRuntime: agentHarness.id,
            allowHarnessAuthProfileForwarding: pluginHarnessOwnsTransport,
            authProfileProvider:
              (lastProfileId
                ? attemptAuthProfileStore.profiles?.[lastProfileId]?.provider
                : undefined) ?? lastProfileId?.split(":", 1)[0],
            authProfileMode: lastProfileId
              ? attemptAuthProfileStore.profiles?.[lastProfileId]?.type
              : undefined,
            sessionAuthProfileId: lastProfileId,
            sessionAuthProfileCandidateIds: pluginHarnessOwnsTransport
              ? pluginHarnessForwardedProfileCandidates
              : undefined,
            config: params.config,
            workspaceDir: resolvedWorkspace,
            agentDir,
            agentId: workspaceResolution.agentId,
            thinkingLevel: thinkLevel,
            extraParamsOverride: {
              ...params.streamParams,
              fastMode: params.fastMode,
            },
          });
          if (!startupStagesEmitted) {
            startupStages.mark("attempt-dispatch");
            notifyExecutionPhase("attempt_dispatch", { provider, model: modelId });
            emitStartupStageSummary("attempt-dispatch");
            startupStagesEmitted = true;
          }

          const attemptAbortController = new AbortController();
          postCompactionAbortController = attemptAbortController;
          const parentAbortSignal = params.abortSignal;
          const relayParentAbort = (): void => {
            attemptAbortController.abort(parentAbortSignal?.reason);
          };
          if (parentAbortSignal?.aborted) {
            relayParentAbort();
          } else {
            parentAbortSignal?.addEventListener("abort", relayParentAbort, { once: true });
          }
          const rawAttempt = await runEmbeddedAttemptWithBackend({
            sessionId: activeSessionId,
            sessionKey: resolvedSessionKey,
            sandboxSessionKey: params.sandboxSessionKey,
            trigger: params.trigger,
            memoryFlushWritePath: params.memoryFlushWritePath,
            messageChannel: params.messageChannel,
            messageProvider: params.messageProvider,
            agentAccountId: params.agentAccountId,
            messageTo: params.messageTo,
            messageThreadId: params.messageThreadId,
            groupId: params.groupId,
            groupChannel: params.groupChannel,
            groupSpace: params.groupSpace,
            memberRoleIds: params.memberRoleIds,
            spawnedBy: params.spawnedBy,
            isCanonicalWorkspace,
            senderId: params.senderId,
            senderName: params.senderName,
            senderUsername: params.senderUsername,
            senderE164: params.senderE164,
            senderIsOwner: params.senderIsOwner,
            currentChannelId: params.currentChannelId,
            currentThreadTs: params.currentThreadTs,
            currentMessageId: params.currentMessageId,
            replyToMode: params.replyToMode,
            hasRepliedRef: params.hasRepliedRef,
            sessionFile: activeSessionFile,
            workspaceDir: resolvedWorkspace,
            agentDir,
            config: params.config,
            allowGatewaySubagentBinding: params.allowGatewaySubagentBinding,
            contextEngine,
            contextTokenBudget: ctxInfo.tokens,
            skillsSnapshot: params.skillsSnapshot,
            prompt,
            transcriptPrompt: params.transcriptPrompt,
            currentTurnContext: params.currentTurnContext,
            images: params.images,
            imageOrder: params.imageOrder,
            clientTools: params.clientTools,
            disableTools: params.disableTools,
            provider,
            modelId,
            // Use the harness selected before model/auth setup for the actual
            // attempt too. Otherwise plugin-owned transports can skip PI auth
            // bootstrap but drift back to PI when the attempt is created.
            agentHarnessId: agentHarness.id,
            runtimePlan,
            model: applyAuthHeaderOverride(
              applyLocalNoAuthHeaderOverride(effectiveModel, apiKeyInfo),
              // When runtime auth exchange produced a different credential
              // (runtimeAuthState is set), the exchanged token lives in
              // authStorage and the SDK will pick it up automatically.
              // Skip header injection to avoid leaking the pre-exchange key.
              runtimeAuthState ? null : apiKeyInfo,
              params.config,
            ),
            resolvedApiKey: resolvedStreamApiKey,
            authProfileId: lastProfileId,
            authProfileIdSource: lockedProfileId ? "user" : "auto",
            initialReplayState: accumulatedReplayState,
            authStorage,
            authProfileStore: runAttemptAuthProfileStore,
            modelRegistry,
            agentId: workspaceResolution.agentId,
            legacyBeforeAgentStartResult,
            thinkLevel,
            onToolOutcome: observePostCompactionToolOutcome,
            fastMode: params.fastMode,
            verboseLevel: params.verboseLevel,
            reasoningLevel: params.reasoningLevel,
            toolResultFormat: resolvedToolResultFormat,
            toolProgressDetail: params.toolProgressDetail,
            execOverrides: params.execOverrides,
            bashElevated: params.bashElevated,
            timeoutMs: params.timeoutMs,
            runId: params.runId,
            abortSignal: attemptAbortController.signal,
            replyOperation: params.replyOperation,
            shouldEmitToolResult: params.shouldEmitToolResult,
            shouldEmitToolOutput: params.shouldEmitToolOutput,
            onPartialReply: params.onPartialReply,
            onAssistantMessageStart: params.onAssistantMessageStart,
            onBlockReply: params.onBlockReply,
            onBlockReplyFlush: params.onBlockReplyFlush,
            blockReplyBreak: params.blockReplyBreak,
            blockReplyChunking: params.blockReplyChunking,
            onReasoningStream: params.onReasoningStream,
            onReasoningEnd: params.onReasoningEnd,
            onToolResult: params.onToolResult,
            onAgentEvent: params.onAgentEvent,
            onExecutionPhase: params.onExecutionPhase,
            extraSystemPrompt: params.extraSystemPrompt,
            sourceReplyDeliveryMode: params.sourceReplyDeliveryMode,
            inputProvenance: params.inputProvenance,
            streamParams: params.streamParams,
            modelRun: params.modelRun,
            promptMode: params.promptMode,
            ownerNumbers: params.ownerNumbers,
            enforceFinalTag: params.enforceFinalTag,
            silentExpected: params.silentExpected,
            bootstrapContextMode: params.bootstrapContextMode,
            bootstrapContextRunKind: params.bootstrapContextRunKind,
            jobId: params.jobId,
            toolsAllow: params.toolsAllow,
            ownerOnlyToolAllowlist: params.ownerOnlyToolAllowlist,
            disableMessageTool: params.disableMessageTool,
            forceMessageTool: params.forceMessageTool,
            enableHeartbeatTool: params.enableHeartbeatTool,
            forceHeartbeatTool: params.forceHeartbeatTool,
            requireExplicitMessageTarget: params.requireExplicitMessageTarget,
            internalEvents: params.internalEvents,
            bootstrapPromptWarningSignaturesSeen,
            bootstrapPromptWarningSignature:
              bootstrapPromptWarningSignaturesSeen[bootstrapPromptWarningSignaturesSeen.length - 1],
            suppressNextUserMessagePersistence,
            onUserMessagePersisted,
          })
            .catch((err: unknown): never => {
              throw postCompactionAbortError ?? err;
            })
            .finally(() => {
              parentAbortSignal?.removeEventListener?.("abort", relayParentAbort);
              if (postCompactionAbortController === attemptAbortController) {
                postCompactionAbortController = undefined;
              }
            });
          if (postCompactionAbortError) {
            throw postCompactionAbortError;
          }
          const attempt = normalizeEmbeddedRunAttemptResult(rawAttempt);

          const {
            aborted,
            externalAbort,
            promptError,
            promptErrorSource,
            preflightRecovery,
            timedOut,
            idleTimedOut,
            timedOutDuringCompaction,
            sessionIdUsed,
            sessionFileUsed,
            lastAssistant: sessionLastAssistant,
            currentAttemptAssistant,
          } = attempt;
          const timedOutDuringToolExecution = attempt.timedOutDuringToolExecution ?? false;
          if (sessionIdUsed && sessionIdUsed !== activeSessionId) {
            activeSessionId = sessionIdUsed;
          }
          if (sessionFileUsed && sessionFileUsed !== activeSessionFile) {
            activeSessionFile = sessionFileUsed;
          }
          bootstrapPromptWarningSignaturesSeen =
            attempt.bootstrapPromptWarningSignaturesSeen ??
            (attempt.bootstrapPromptWarningSignature
              ? Array.from(
                  new Set([
                    ...bootstrapPromptWarningSignaturesSeen,
                    attempt.bootstrapPromptWarningSignature,
                  ]),
                )
              : bootstrapPromptWarningSignaturesSeen);
          const lastAssistantUsage = normalizeUsage(sessionLastAssistant?.usage as UsageLike);
          const attemptUsage = attempt.attemptUsage ?? lastAssistantUsage;
          mergeUsageIntoAccumulator(usageAccumulator, attemptUsage);
          // Keep prompt size from the latest model call so session totalTokens
          // reflects current context usage, not accumulated tool-loop usage.
          lastRunPromptUsage = lastAssistantUsage ?? attemptUsage;
          lastTurnTotal = lastAssistantUsage?.total ?? attemptUsage?.total;
          // Idle-timeout cost-runaway breaker (#76293). Logic lives in the
          // pure helper below so it stays unit-testable; the run loop just
          // feeds it the latest attempt outcome and bails through the
          // existing retry-limit exhaustion path when the cap is hit.
          const breakerStep = stepIdleTimeoutBreaker(idleTimeoutBreakerState, {
            idleTimedOut,
            completedModelProgress: hasCompletedModelProgressForIdleBreaker(attempt),
            outputTokens: attemptUsage?.output,
          });
          if (breakerStep.tripped) {
            const breakerMessage =
              `Idle-timeout cost-runaway breaker tripped: ` +
              `${breakerStep.consecutive} consecutive idle timeouts ` +
              `without completed model progress ` +
              `(cap=${MAX_CONSECUTIVE_IDLE_TIMEOUTS_BEFORE_OUTPUT}). ` +
              `Halting further attempts to bound paid model calls. ` +
              `See issue #76293.`;
            log.error(
              `[idle-timeout-circuit-breaker-tripped] ` +
                `sessionKey=${params.sessionKey ?? params.sessionId} ` +
                `provider=${provider}/${modelId} ` +
                `consecutive=${breakerStep.consecutive} ` +
                `cap=${MAX_CONSECUTIVE_IDLE_TIMEOUTS_BEFORE_OUTPUT}`,
            );
            const breakerDecision = resolveRunFailoverDecision({
              stage: "retry_limit",
              fallbackConfigured,
              failoverReason: lastRetryFailoverReason,
            });
            return handleRetryLimitExhaustion({
              message: breakerMessage,
              decision: breakerDecision,
              provider,
              model: modelId,
              profileId: lastProfileId,
              durationMs: Date.now() - started,
              agentMeta: buildErrorAgentMeta({
                sessionId: activeSessionId,
                provider,
                model: model.id,
                contextTokens: ctxInfo.tokens,
                usageAccumulator,
                lastRunPromptUsage,
                lastTurnTotal,
              }),
              replayInvalid: accumulatedReplayState.replayInvalid ? true : undefined,
              livenessState: "blocked",
            });
          }
          const attemptCompactionCount = Math.max(0, attempt.compactionCount ?? 0);
          autoCompactionCount += attemptCompactionCount;
          if (
            typeof attempt.compactionTokensAfter === "number" &&
            Number.isFinite(attempt.compactionTokensAfter) &&
            attempt.compactionTokensAfter > 0
          ) {
            lastCompactionTokensAfter = Math.floor(attempt.compactionTokensAfter);
          }
          const activeErrorContext = resolveActiveErrorContext({
            provider,
            model: modelId,
            assistant: currentAttemptAssistant ?? sessionLastAssistant,
          });
          const resolveReplayInvalidForAttempt = (incompleteTurnText?: string | null) =>
            accumulatedReplayState.replayInvalid ||
            resolveReplayInvalidFlag({
              attempt,
              incompleteTurnText,
            });
          if (resolveReplayInvalidForAttempt(null)) {
            accumulatedReplayState.replayInvalid = true;
          }
          accumulatedReplayState = observeReplayMetadata(
            accumulatedReplayState,
            attempt.replayMetadata,
          );
          const formattedAssistantErrorText = sessionLastAssistant
            ? formatAssistantErrorText(sessionLastAssistant, {
                cfg: params.config,
                sessionKey: resolvedSessionKey ?? params.sessionId,
                provider: activeErrorContext.provider,
                model: activeErrorContext.model,
              })
            : undefined;
          const assistantErrorText =
            sessionLastAssistant?.stopReason === "error"
              ? sessionLastAssistant.errorMessage?.trim() || formattedAssistantErrorText
              : undefined;
          const canRestartForLiveSwitch =
            !hasMessagingToolDeliveryEvidence(attempt) &&
            !attempt.didSendDeterministicApprovalPrompt &&
            !attempt.lastToolError &&
            (attempt.toolMetas?.length ?? 0) === 0 &&
            (attempt.assistantTexts?.length ?? 0) === 0;
          if (preflightRecovery?.handled) {
            const retryingFromTranscript = preflightRecovery.source === "mid-turn";
            log.info(
              `[context-overflow-precheck] early recovery route=${preflightRecovery.route} ` +
                `completed for ${provider}/${modelId}; ` +
                (retryingFromTranscript ? "retrying from current transcript" : "retrying prompt"),
            );
            if (retryingFromTranscript) {
              continueFromCurrentTranscript();
            }
            continue;
          }
          const requestedSelection = shouldSwitchToLiveModel({
            cfg: params.config,
            sessionKey: resolvedSessionKey,
            agentId: params.agentId,
            defaultProvider: DEFAULT_PROVIDER,
            defaultModel: DEFAULT_MODEL,
            currentProvider: provider,
            currentModel: modelId,
            currentAuthProfileId: preferredProfileId,
            currentAuthProfileIdSource: params.authProfileIdSource,
          });
          if (requestedSelection && canRestartForLiveSwitch) {
            await clearLiveModelSwitchPending({
              cfg: params.config,
              sessionKey: resolvedSessionKey,
              agentId: params.agentId,
            });
            log.info(
              `live session model switch requested during active attempt for ${params.sessionId}: ${provider}/${modelId} -> ${requestedSelection.provider}/${requestedSelection.model}`,
            );
            throw new LiveSessionModelSwitchError(requestedSelection);
          }
          // ── Timeout-triggered compaction ──────────────────────────────────
          // When the LLM times out with high context usage, compact before
          // retrying to break the death spiral of repeated timeouts.
          if (timedOut && !timedOutDuringCompaction && !timedOutDuringToolExecution) {
            // Only consider prompt-side tokens here. API totals include output
            // tokens, which can make a long generation look like high context
            // pressure even when the prompt itself was small.
            const lastTurnPromptTokens = derivePromptTokens(lastRunPromptUsage);
            const tokenUsedRatio =
              lastTurnPromptTokens != null && ctxInfo.tokens > 0
                ? lastTurnPromptTokens / ctxInfo.tokens
                : 0;
            if (timeoutCompactionAttempts >= MAX_TIMEOUT_COMPACTION_ATTEMPTS) {
              log.warn(
                `[timeout-compaction] already attempted timeout compaction ${timeoutCompactionAttempts} time(s); falling through to failover rotation`,
              );
            } else if (tokenUsedRatio > 0.65) {
              const timeoutDiagId = createCompactionDiagId();
              timeoutCompactionAttempts++;
              log.warn(
                `[timeout-compaction] LLM timed out with high prompt token usage (${Math.round(tokenUsedRatio * 100)}%); ` +
                  `attempting compaction before retry (attempt ${timeoutCompactionAttempts}/${MAX_TIMEOUT_COMPACTION_ATTEMPTS}) diagId=${timeoutDiagId}`,
              );
              let timeoutCompactResult: Awaited<ReturnType<typeof contextEngine.compact>>;
              await runOwnsCompactionBeforeHook("timeout recovery");
              try {
                const timeoutCompactionRuntimeContext = {
                  ...buildEmbeddedCompactionRuntimeContext({
                    sessionKey: params.sessionKey,
                    messageChannel: params.messageChannel,
                    messageProvider: params.messageProvider,
                    agentAccountId: params.agentAccountId,
                    currentChannelId: params.currentChannelId,
                    currentThreadTs: params.currentThreadTs,
                    currentMessageId: params.currentMessageId,
                    authProfileId: lastProfileId,
                    workspaceDir: resolvedWorkspace,
                    agentDir,
                    config: params.config,
                    skillsSnapshot: params.skillsSnapshot,
                    senderIsOwner: params.senderIsOwner,
                    senderId: params.senderId,
                    provider,
                    modelId,
                    modelFallbacksOverride: params.modelFallbacksOverride,
                    thinkLevel,
                    reasoningLevel: params.reasoningLevel,
                    bashElevated: params.bashElevated,
                    extraSystemPrompt: params.extraSystemPrompt,
                    sourceReplyDeliveryMode: params.sourceReplyDeliveryMode,
                    ownerNumbers: params.ownerNumbers,
                    activeProcessSessions: listActiveProcessSessionReferences({
                      scopeKey: resolveProcessToolScopeKey({
                        sessionKey: params.sandboxSessionKey?.trim() || params.sessionKey,
                        sessionId: activeSessionId,
                        agentId: sessionAgentId,
                      }),
                    }),
                  }),
                  ...resolveContextEngineCapabilities({
                    config: params.config,
                    sessionKey: params.sessionKey,
                    agentId: sessionAgentId,
                    contextEnginePluginId,
                    purpose: "context-engine.timeout-compaction",
                  }),
                  onCompactionHookMessages,
                  ...(attempt.promptCache ? { promptCache: attempt.promptCache } : {}),
                  runId: params.runId,
                  trigger: "timeout_recovery",
                  diagId: timeoutDiagId,
                  attempt: timeoutCompactionAttempts,
                  maxAttempts: MAX_TIMEOUT_COMPACTION_ATTEMPTS,
                };
                timeoutCompactResult = await contextEngine.compact({
                  sessionId: activeSessionId,
                  sessionKey: params.sessionKey,
                  sessionFile: activeSessionFile,
                  tokenBudget: ctxInfo.tokens,
                  force: true,
                  compactionTarget: "budget",
                  runtimeContext: timeoutCompactionRuntimeContext,
                });
              } catch (compactErr) {
                log.warn(
                  `[timeout-compaction] contextEngine.compact() threw during timeout recovery for ${provider}/${modelId}: ${String(compactErr)}`,
                );
                timeoutCompactResult = {
                  ok: false,
                  compacted: false,
                  reason: String(compactErr),
                };
              }
              if (timeoutCompactResult.compacted) {
                adoptCompactionTranscript(timeoutCompactResult);
              }
              await runOwnsCompactionAfterHook("timeout recovery", timeoutCompactResult);
              if (timeoutCompactResult.compacted) {
                autoCompactionCount += 1;
                if (
                  typeof timeoutCompactResult.result?.tokensAfter === "number" &&
                  Number.isFinite(timeoutCompactResult.result.tokensAfter) &&
                  timeoutCompactResult.result.tokensAfter > 0
                ) {
                  lastCompactionTokensAfter = Math.floor(timeoutCompactResult.result.tokensAfter);
                }
                if (contextEngine.info.ownsCompaction === true) {
                  await runPostCompactionSideEffects({
                    config: params.config,
                    sessionKey: params.sessionKey,
                    sessionFile: activeSessionFile,
                  });
                }
                log.info(
                  `[timeout-compaction] compaction succeeded for ${provider}/${modelId}; retrying prompt`,
                );
                postCompactionGuard.armPostCompaction();
                continue;
              } else {
                log.warn(
                  `[timeout-compaction] compaction did not reduce context for ${provider}/${modelId}; falling through to normal handling`,
                );
              }
            }
          }

          const contextOverflowError = !aborted
            ? (() => {
                if (promptError) {
                  const errorText = formatErrorMessage(promptError);
                  if (isLikelyContextOverflowError(errorText)) {
                    return { text: errorText, source: "promptError" as const };
                  }
                  // Prompt submission failed with a non-overflow error. Do not
                  // inspect prior assistant errors from history for this attempt.
                  return null;
                }
                if (assistantErrorText && isLikelyContextOverflowError(assistantErrorText)) {
                  return {
                    text: assistantErrorText,
                    source: "assistantError" as const,
                  };
                }
                return null;
              })()
            : null;

          if (contextOverflowError) {
            const overflowDiagId = createCompactionDiagId();
            const errorText = contextOverflowError.text;
            const msgCount = attempt.messagesSnapshot?.length ?? 0;
            const observedOverflowTokens = extractObservedOverflowTokenCount(errorText);
            log.warn(
              `[context-overflow-diag] sessionKey=${params.sessionKey ?? params.sessionId} ` +
                `provider=${provider}/${modelId} source=${contextOverflowError.source} ` +
                `messages=${msgCount} sessionFile=${activeSessionFile} ` +
                `diagId=${overflowDiagId} compactionAttempts=${overflowCompactionAttempts} ` +
                `observedTokens=${observedOverflowTokens ?? "unknown"} ` +
                `error=${errorText.slice(0, 200)}`,
            );
            const isCompactionFailure = isCompactionFailureError(errorText);
            const hadAttemptLevelCompaction = attemptCompactionCount > 0;
            // If this attempt already compacted (SDK auto-compaction), avoid immediately
            // running another explicit compaction for the same overflow trigger.
            if (
              !isCompactionFailure &&
              hadAttemptLevelCompaction &&
              overflowCompactionAttempts < MAX_OVERFLOW_COMPACTION_ATTEMPTS
            ) {
              overflowCompactionAttempts++;
              log.warn(
                `context overflow persisted after in-attempt compaction (attempt ${overflowCompactionAttempts}/${MAX_OVERFLOW_COMPACTION_ATTEMPTS}); retrying prompt without additional compaction for ${provider}/${modelId}`,
              );
              if (preflightRecovery?.source === "mid-turn") {
                continueFromCurrentTranscript();
              }
              continue;
            }
            // Attempt explicit overflow compaction only when this attempt did not
            // already auto-compact.
            if (
              !isCompactionFailure &&
              !hadAttemptLevelCompaction &&
              overflowCompactionAttempts < MAX_OVERFLOW_COMPACTION_ATTEMPTS
            ) {
              if (log.isEnabled("debug")) {
                log.debug(
                  `[compaction-diag] decision diagId=${overflowDiagId} branch=compact ` +
                    `isCompactionFailure=${isCompactionFailure} hasOversizedToolResults=unknown ` +
                    `attempt=${overflowCompactionAttempts + 1} maxAttempts=${MAX_OVERFLOW_COMPACTION_ATTEMPTS}`,
                );
              }
              overflowCompactionAttempts++;
              log.warn(
                `context overflow detected (attempt ${overflowCompactionAttempts}/${MAX_OVERFLOW_COMPACTION_ATTEMPTS}); attempting auto-compaction for ${provider}/${modelId}`,
              );
              let compactResult: Awaited<ReturnType<typeof contextEngine.compact>>;
              await runOwnsCompactionBeforeHook("overflow recovery");
              try {
                const overflowCompactionRuntimeContext = {
                  ...buildEmbeddedCompactionRuntimeContext({
                    sessionKey: params.sessionKey,
                    messageChannel: params.messageChannel,
                    messageProvider: params.messageProvider,
                    agentAccountId: params.agentAccountId,
                    currentChannelId: params.currentChannelId,
                    currentThreadTs: params.currentThreadTs,
                    currentMessageId: params.currentMessageId,
                    authProfileId: lastProfileId,
                    workspaceDir: resolvedWorkspace,
                    agentDir,
                    config: params.config,
                    skillsSnapshot: params.skillsSnapshot,
                    senderIsOwner: params.senderIsOwner,
                    senderId: params.senderId,
                    provider,
                    modelId,
                    thinkLevel,
                    reasoningLevel: params.reasoningLevel,
                    bashElevated: params.bashElevated,
                    extraSystemPrompt: params.extraSystemPrompt,
                    sourceReplyDeliveryMode: params.sourceReplyDeliveryMode,
                    ownerNumbers: params.ownerNumbers,
                    activeProcessSessions: listActiveProcessSessionReferences({
                      scopeKey: resolveProcessToolScopeKey({
                        sessionKey: params.sandboxSessionKey?.trim() || params.sessionKey,
                        sessionId: activeSessionId,
                        agentId: sessionAgentId,
                      }),
                    }),
                  }),
                  ...resolveContextEngineCapabilities({
                    config: params.config,
                    sessionKey: params.sessionKey,
                    agentId: sessionAgentId,
                    contextEnginePluginId,
                    purpose: "context-engine.overflow-compaction",
                  }),
                  onCompactionHookMessages,
                  ...(attempt.promptCache ? { promptCache: attempt.promptCache } : {}),
                  runId: params.runId,
                  trigger: "overflow",
                  ...(observedOverflowTokens !== undefined
                    ? { currentTokenCount: observedOverflowTokens }
                    : {}),
                  diagId: overflowDiagId,
                  attempt: overflowCompactionAttempts,
                  maxAttempts: MAX_OVERFLOW_COMPACTION_ATTEMPTS,
                };
                compactResult = await contextEngine.compact({
                  sessionId: activeSessionId,
                  sessionKey: params.sessionKey,
                  sessionFile: activeSessionFile,
                  tokenBudget: ctxInfo.tokens,
                  ...(observedOverflowTokens !== undefined
                    ? { currentTokenCount: observedOverflowTokens }
                    : {}),
                  force: true,
                  compactionTarget: "budget",
                  runtimeContext: overflowCompactionRuntimeContext,
                });
                if (compactResult.ok && compactResult.compacted) {
                  adoptCompactionTranscript(compactResult);
                  await runContextEngineMaintenance({
                    contextEngine,
                    sessionId: activeSessionId,
                    sessionKey: params.sessionKey,
                    sessionFile: activeSessionFile,
                    reason: "compaction",
                    runtimeContext: overflowCompactionRuntimeContext,
                    config: params.config,
                    agentId: sessionAgentId,
                  });
                }
              } catch (compactErr) {
                log.warn(
                  `contextEngine.compact() threw during overflow recovery for ${provider}/${modelId}: ${String(compactErr)}`,
                );
                compactResult = {
                  ok: false,
                  compacted: false,
                  reason: String(compactErr),
                };
              }
              await runOwnsCompactionAfterHook("overflow recovery", compactResult);
              if (compactResult.compacted) {
                adoptCompactionTranscript(compactResult);
                if (
                  typeof compactResult.result?.tokensAfter === "number" &&
                  Number.isFinite(compactResult.result.tokensAfter) &&
                  compactResult.result.tokensAfter > 0
                ) {
                  lastCompactionTokensAfter = Math.floor(compactResult.result.tokensAfter);
                }
                if (preflightRecovery?.route === "compact_then_truncate") {
                  const truncResult = await truncateOversizedToolResultsInSession({
                    sessionFile: activeSessionFile,
                    contextWindowTokens: ctxInfo.tokens,
                    maxCharsOverride: resolveLiveToolResultMaxChars({
                      contextWindowTokens: ctxInfo.tokens,
                      cfg: params.config,
                      agentId: sessionAgentId,
                    }),
                    sessionId: activeSessionId,
                    sessionKey: params.sessionKey,
                    config: params.config,
                  });
                  if (truncResult.truncated) {
                    log.info(
                      `[context-overflow-precheck] post-compaction tool-result truncation succeeded for ` +
                        `${provider}/${modelId}; truncated ${truncResult.truncatedCount} tool result(s)`,
                    );
                  } else {
                    log.warn(
                      `[context-overflow-precheck] post-compaction tool-result truncation did not help for ` +
                        `${provider}/${modelId}: ${truncResult.reason ?? "unknown"}`,
                    );
                  }
                }
                autoCompactionCount += 1;
                log.info(`auto-compaction succeeded for ${provider}/${modelId}; retrying prompt`);
                postCompactionGuard.armPostCompaction();
                if (preflightRecovery?.source === "mid-turn") {
                  continueFromCurrentTranscript();
                } else if (
                  params.currentMessageId !== undefined &&
                  params.currentMessageId === lastPersistedCurrentMessageId
                ) {
                  // The first attempt reached Pi far enough to persist this user turn.
                  // Retrying the original prompt would replay it, so resume from the
                  // compacted transcript and suppress the next user append.
                  nextAttemptPromptOverride = MID_TURN_PRECHECK_CONTINUATION_PROMPT;
                  suppressNextUserMessagePersistence = true;
                }
                continue;
              }
              log.warn(
                `auto-compaction failed for ${provider}/${modelId}: ${compactResult.reason ?? "nothing to compact"}`,
              );
            }
            if (!toolResultTruncationAttempted) {
              const contextWindowTokens = ctxInfo.tokens;
              const toolResultMaxChars = resolveLiveToolResultMaxChars({
                contextWindowTokens,
                cfg: params.config,
                agentId: sessionAgentId,
              });
              const hasOversized = attempt.messagesSnapshot
                ? sessionLikelyHasOversizedToolResults({
                    messages: attempt.messagesSnapshot,
                    contextWindowTokens,
                    maxCharsOverride: toolResultMaxChars,
                  })
                : false;

              if (hasOversized) {
                toolResultTruncationAttempted = true;
                log.warn(
                  `[context-overflow-recovery] Attempting tool result truncation for ${provider}/${modelId} ` +
                    `(contextWindow=${contextWindowTokens} tokens)`,
                );
                const truncResult = await truncateOversizedToolResultsInSession({
                  sessionFile: activeSessionFile,
                  contextWindowTokens,
                  maxCharsOverride: toolResultMaxChars,
                  sessionId: activeSessionId,
                  sessionKey: params.sessionKey,
                  config: params.config,
                });
                if (truncResult.truncated) {
                  log.info(
                    `[context-overflow-recovery] Truncated ${truncResult.truncatedCount} tool result(s); retrying prompt`,
                  );
                  if (preflightRecovery?.source === "mid-turn") {
                    continueFromCurrentTranscript();
                  }
                  continue;
                }
                log.warn(
                  `[context-overflow-recovery] Tool result truncation did not help: ${truncResult.reason ?? "unknown"}`,
                );
              }
            }
            if (
              (isCompactionFailure ||
                overflowCompactionAttempts >= MAX_OVERFLOW_COMPACTION_ATTEMPTS) &&
              log.isEnabled("debug")
            ) {
              log.debug(
                `[compaction-diag] decision diagId=${overflowDiagId} branch=give_up ` +
                  `isCompactionFailure=${isCompactionFailure} hasOversizedToolResults=unknown ` +
                  `attempt=${overflowCompactionAttempts} maxAttempts=${MAX_OVERFLOW_COMPACTION_ATTEMPTS}`,
              );
            }
            const kind = isCompactionFailure ? "compaction_failure" : "context_overflow";
            attempt.setTerminalLifecycleMeta?.({
              replayInvalid: resolveReplayInvalidForAttempt(),
              livenessState: "blocked",
            });
            return {
              payloads: [
                {
                  text:
                    "Context overflow: prompt too large for the model. " +
                    "Try /reset (or /new) to start a fresh session, or use a larger-context model.",
                  isError: true,
                },
              ],
              meta: {
                durationMs: Date.now() - started,
                agentMeta: buildErrorAgentMeta({
                  sessionId: sessionIdUsed,
                  provider,
                  model: model.id,
                  contextTokens: ctxInfo.tokens,
                  usageAccumulator,
                  lastRunPromptUsage,
                  lastAssistant: sessionLastAssistant,
                  lastTurnTotal,
                }),
                systemPromptReport: attempt.systemPromptReport,
                finalPromptText: attempt.finalPromptText,
                replayInvalid: resolveReplayInvalidForAttempt(),
                livenessState: "blocked",
                error: { kind, message: errorText },
              },
            };
          }

          if (promptErrorSource === "hook:before_agent_run" && !aborted) {
            const errorText = formatErrorMessage(promptError);
            const replayInvalid = resolveReplayInvalidForAttempt();
            attempt.setTerminalLifecycleMeta?.({
              replayInvalid,
              livenessState: "blocked",
            });
            return {
              payloads: [{ text: errorText, isError: true }],
              meta: {
                durationMs: Date.now() - started,
                agentMeta: buildErrorAgentMeta({
                  sessionId: sessionIdUsed,
                  provider,
                  model: model.id,
                  contextTokens: ctxInfo.tokens,
                  usageAccumulator,
                  lastRunPromptUsage,
                  lastAssistant: sessionLastAssistant,
                  lastTurnTotal,
                }),
                systemPromptReport: attempt.systemPromptReport,
                finalAssistantVisibleText: errorText,
                finalAssistantRawText: errorText,
                finalPromptText: undefined,
                replayInvalid,
                livenessState: "blocked",
                error: { kind: "hook_block", message: errorText },
              },
            };
          }

          if (promptError && !aborted && promptErrorSource !== "compaction") {
            // Normalize wrapped errors (e.g. abort-wrapped RESOURCE_EXHAUSTED) into
            // FailoverError so rate-limit classification works even for nested shapes.
            //
            // promptErrorSource === "compaction" means the model call already completed and the
            // abort happened only while waiting for compaction/retry cleanup. Retrying from here
            // would replay that completed tool turn as a fresh prompt attempt.
            const normalizedPromptFailover = coerceToFailoverError(promptError, {
              provider: activeErrorContext.provider,
              model: activeErrorContext.model,
              profileId: lastProfileId,
              sessionId: sessionIdUsed,
              lane: globalLane,
            });
            const promptErrorDetails = normalizedPromptFailover
              ? describeFailoverError(normalizedPromptFailover)
              : describeFailoverError(promptError);
            if (normalizedPromptFailover?.suspend) {
              void suspendSession({
                cfg: params.config,
                agentDir,
                sessionId: activeSessionId ?? params.sessionId,
                laneId: globalLane,
                reason: resolveSessionSuspensionReason(normalizedPromptFailover.reason),
                failedProvider: normalizedPromptFailover.provider ?? provider,
                failedModel: normalizedPromptFailover.model ?? modelId,
              });
            }
            const errorText = promptErrorDetails.message || formatErrorMessage(promptError);
            if (await maybeRefreshRuntimeAuthForAuthError(errorText, runtimeAuthRetry)) {
              authRetryPending = true;
              continue;
            }
            // Handle role ordering errors with a user-friendly message
            if (/incorrect role information|roles must alternate/i.test(errorText)) {
              attempt.setTerminalLifecycleMeta?.({
                replayInvalid: resolveReplayInvalidForAttempt(),
                livenessState: "blocked",
              });
              return {
                payloads: [
                  {
                    text:
                      "Message ordering conflict - please try again. " +
                      "If this persists, use /new to start a fresh session.",
                    isError: true,
                  },
                ],
                meta: {
                  durationMs: Date.now() - started,
                  agentMeta: buildErrorAgentMeta({
                    sessionId: sessionIdUsed,
                    provider,
                    model: model.id,
                    contextTokens: ctxInfo.tokens,
                    usageAccumulator,
                    lastRunPromptUsage,
                    lastAssistant: sessionLastAssistant,
                    lastTurnTotal,
                  }),
                  systemPromptReport: attempt.systemPromptReport,
                  finalPromptText: attempt.finalPromptText,
                  replayInvalid: resolveReplayInvalidForAttempt(),
                  livenessState: "blocked",
                  error: { kind: "role_ordering", message: errorText },
                },
              };
            }
            // Handle image size errors with a user-friendly message (no retry needed)
            const imageSizeError = parseImageSizeError(errorText);
            if (imageSizeError) {
              const maxMb = imageSizeError.maxMb;
              const maxMbLabel =
                typeof maxMb === "number" && Number.isFinite(maxMb) ? `${maxMb}` : null;
              const maxBytesHint = maxMbLabel ? ` (max ${maxMbLabel}MB)` : "";
              attempt.setTerminalLifecycleMeta?.({
                replayInvalid: resolveReplayInvalidForAttempt(),
                livenessState: "blocked",
              });
              return {
                payloads: [
                  {
                    text:
                      `Image too large for the model${maxBytesHint}. ` +
                      "Please compress or resize the image and try again.",
                    isError: true,
                  },
                ],
                meta: {
                  durationMs: Date.now() - started,
                  agentMeta: buildErrorAgentMeta({
                    sessionId: sessionIdUsed,
                    provider,
                    model: model.id,
                    contextTokens: ctxInfo.tokens,
                    usageAccumulator,
                    lastRunPromptUsage,
                    lastAssistant: sessionLastAssistant,
                    lastTurnTotal,
                  }),
                  systemPromptReport: attempt.systemPromptReport,
                  finalPromptText: attempt.finalPromptText,
                  replayInvalid: resolveReplayInvalidForAttempt(),
                  livenessState: "blocked",
                  error: { kind: "image_size", message: errorText },
                },
              };
            }
            const promptFailoverReason =
              promptErrorDetails.reason ?? classifyFailoverReason(errorText, { provider });
            const promptProfileFailureReason =
              resolveRunAuthProfileFailureReason(promptFailoverReason);
            const promptFailoverFailure =
              promptFailoverReason !== null || isFailoverErrorMessage(errorText, { provider });
            // Capture the failing profile before auth-profile rotation mutates `lastProfileId`.
            const failedPromptProfileId = lastProfileId;
            const logPromptFailoverDecision = createFailoverDecisionLogger({
              stage: "prompt",
              runId: params.runId,
              rawError: errorText,
              failoverReason: promptFailoverReason,
              profileFailureReason: promptProfileFailureReason,
              provider,
              model: modelId,
              sourceProvider: provider,
              sourceModel: modelId,
              profileId: failedPromptProfileId,
              fallbackConfigured,
              aborted,
            });
            if (promptFailoverReason === "rate_limit") {
              maybeEscalateRateLimitProfileFallback({
                failoverProvider: provider,
                failoverModel: modelId,
                logFallbackDecision: logPromptFailoverDecision,
              });
            }
            let promptFailoverDecision = resolveRunFailoverDecision({
              stage: "prompt",
              aborted,
              externalAbort,
              fallbackConfigured,
              failoverFailure: promptFailoverFailure,
              failoverReason: promptFailoverReason,
              profileRotated: false,
            });
            if (
              promptFailoverDecision.action === "rotate_profile" &&
              (await (pluginHarnessOwnsTransport
                ? advancePluginHarnessAuthProfile()
                : advanceAuthProfile()))
            ) {
              if (failedPromptProfileId && promptProfileFailureReason) {
                void maybeMarkAuthProfileFailure({
                  profileId: failedPromptProfileId,
                  reason: promptProfileFailureReason,
                  modelId,
                }).catch((err) => {
                  log.warn(`prompt profile failure mark failed: ${String(err)}`);
                });
              }
              traceAttempts.push({
                provider,
                model: modelId,
                result: promptFailoverReason === "timeout" ? "timeout" : "rotate_profile",
                ...(promptFailoverReason ? { reason: promptFailoverReason } : {}),
                stage: "prompt",
              });
              lastRetryFailoverReason = mergeRetryFailoverReason({
                previous: lastRetryFailoverReason,
                failoverReason: promptFailoverReason,
              });
              logPromptFailoverDecision("rotate_profile");
              await maybeBackoffBeforeOverloadFailover(promptFailoverReason);
              continue;
            }
            if (promptFailoverDecision.action === "rotate_profile") {
              promptFailoverDecision = resolveRunFailoverDecision({
                stage: "prompt",
                aborted,
                externalAbort,
                fallbackConfigured,
                failoverFailure: promptFailoverFailure,
                failoverReason: promptFailoverReason,
                profileRotated: true,
              });
            }
            if (failedPromptProfileId && promptProfileFailureReason) {
              try {
                await maybeMarkAuthProfileFailure({
                  profileId: failedPromptProfileId,
                  reason: promptProfileFailureReason,
                  modelId,
                });
              } catch (err) {
                log.warn(`prompt profile failure mark failed: ${String(err)}`);
              }
            }
            const fallbackThinking = pickFallbackThinkingLevel({
              message: errorText,
              attempted: attemptedThinking,
            });
            if (fallbackThinking) {
              log.warn(
                `unsupported thinking level for ${provider}/${modelId}; retrying with ${fallbackThinking}`,
              );
              thinkLevel = fallbackThinking;
              continue;
            }
            // Throw FailoverError for prompt-side failover reasons when fallbacks
            // are configured so outer model fallback can continue on overload,
            // rate-limit, auth, or billing failures.
            if (promptFailoverDecision.action === "fallback_model") {
              const fallbackReason = promptFailoverDecision.reason ?? "unknown";
              const status = resolveFailoverStatus(fallbackReason);
              traceAttempts.push({
                provider,
                model: modelId,
                result: promptFailoverReason === "timeout" ? "timeout" : "fallback_model",
                reason: fallbackReason,
                stage: "prompt",
                ...(typeof status === "number" ? { status } : {}),
              });
              logPromptFailoverDecision("fallback_model", { status });
              await maybeBackoffBeforeOverloadFailover(promptFailoverReason);
              throw (
                normalizedPromptFailover ??
                new FailoverError(errorText, {
                  reason: fallbackReason,
                  provider,
                  model: modelId,
                  profileId: lastProfileId,
                  sessionId: sessionIdUsed,
                  lane: globalLane,
                  status,
                })
              );
            }
            if (promptFailoverDecision.action === "surface_error") {
              traceAttempts.push({
                provider,
                model: modelId,
                result: promptFailoverReason === "timeout" ? "timeout" : "surface_error",
                ...(promptFailoverReason ? { reason: promptFailoverReason } : {}),
                stage: "prompt",
              });
              logPromptFailoverDecision("surface_error");
            }
            throw promptError;
          }

          const assistantForFailover = currentAttemptAssistant ?? sessionLastAssistant;
          const fallbackThinking = pickFallbackThinkingLevel({
            message: assistantForFailover?.errorMessage,
            attempted: attemptedThinking,
          });
          if (fallbackThinking && !aborted) {
            log.warn(
              `unsupported thinking level for ${provider}/${modelId}; retrying with ${fallbackThinking}`,
            );
            thinkLevel = fallbackThinking;
            continue;
          }

          const authFailure = isAuthAssistantError(assistantForFailover);
          const rateLimitFailure = isRateLimitAssistantError(assistantForFailover);
          const billingFailure = isBillingAssistantError(assistantForFailover);
          const failoverFailure = isFailoverAssistantError(assistantForFailover);
          const assistantFailoverReason = classifyFailoverReason(
            assistantForFailover?.errorMessage ?? "",
            {
              provider: assistantForFailover?.provider,
            },
          );
          const assistantProfileFailureReason =
            resolveRunAuthProfileFailureReason(assistantFailoverReason);
          const cloudCodeAssistFormatError = attempt.cloudCodeAssistFormatError;
          const imageDimensionError = parseImageDimensionError(
            assistantForFailover?.errorMessage ?? "",
          );
          // Capture the failing profile before auth-profile rotation mutates `lastProfileId`.
          const failedAssistantProfileId = lastProfileId;
          const logAssistantFailoverDecision = createFailoverDecisionLogger({
            stage: "assistant",
            runId: params.runId,
            rawError: assistantForFailover?.errorMessage?.trim(),
            failoverReason: assistantFailoverReason,
            profileFailureReason: assistantProfileFailureReason,
            provider: activeErrorContext.provider,
            model: activeErrorContext.model,
            sourceProvider: assistantForFailover?.provider ?? provider,
            sourceModel: assistantForFailover?.model ?? modelId,
            profileId: failedAssistantProfileId,
            fallbackConfigured,
            timedOut,
            aborted,
          });

          if (
            authFailure &&
            (await maybeRefreshRuntimeAuthForAuthError(
              assistantForFailover?.errorMessage ?? "",
              runtimeAuthRetry,
            ))
          ) {
            authRetryPending = true;
            continue;
          }
          if (imageDimensionError && lastProfileId) {
            const details = [
              imageDimensionError.messageIndex !== undefined
                ? `message=${imageDimensionError.messageIndex}`
                : null,
              imageDimensionError.contentIndex !== undefined
                ? `content=${imageDimensionError.contentIndex}`
                : null,
              imageDimensionError.maxDimensionPx !== undefined
                ? `limit=${imageDimensionError.maxDimensionPx}px`
                : null,
            ]
              .filter(Boolean)
              .join(" ");
            log.warn(
              `Profile ${lastProfileId} rejected image payload${details ? ` (${details})` : ""}.`,
            );
          }

          const assistantFailoverDecision = resolveRunFailoverDecision({
            stage: "assistant",
            allowFormatRetry: cloudCodeAssistFormatError,
            aborted,
            externalAbort,
            fallbackConfigured,
            failoverFailure,
            failoverReason: assistantFailoverReason,
            timedOut,
            timedOutDuringCompaction,
            timedOutDuringToolExecution,
            profileRotated: false,
          });
          const assistantFailoverOutcome = await handleAssistantFailover({
            initialDecision: assistantFailoverDecision,
            aborted,
            externalAbort,
            fallbackConfigured,
            failoverFailure,
            failoverReason: assistantFailoverReason,
            timedOut,
            idleTimedOut,
            timedOutDuringCompaction,
            timedOutDuringToolExecution,
            allowSameModelIdleTimeoutRetry:
              timedOut &&
              idleTimedOut &&
              !timedOutDuringCompaction &&
              !fallbackConfigured &&
              canRestartForLiveSwitch &&
              sameModelIdleTimeoutRetries < MAX_SAME_MODEL_IDLE_TIMEOUT_RETRIES,
            assistantProfileFailureReason,
            lastProfileId,
            modelId,
            provider,
            activeErrorContext,
            lastAssistant: assistantForFailover,
            config: params.config,
            sessionKey: params.sessionKey ?? params.sessionId,
            authFailure,
            rateLimitFailure,
            billingFailure,
            cloudCodeAssistFormatError,
            isProbeSession,
            overloadProfileRotations,
            overloadProfileRotationLimit,
            previousRetryFailoverReason: lastRetryFailoverReason,
            logAssistantFailoverDecision,
            warn: (message) => log.warn(message),
            maybeMarkAuthProfileFailure,
            maybeEscalateRateLimitProfileFallback,
            maybeBackoffBeforeOverloadFailover,
            advanceAuthProfile: pluginHarnessOwnsTransport
              ? advancePluginHarnessAuthProfile
              : advanceAuthProfile,
          });
          overloadProfileRotations = assistantFailoverOutcome.overloadProfileRotations;
          if (assistantFailoverOutcome.action === "retry") {
            traceAttempts.push({
              provider: activeErrorContext.provider,
              model: activeErrorContext.model,
              result:
                assistantFailoverOutcome.retryKind === "same_model_idle_timeout" ||
                assistantFailoverReason === "timeout"
                  ? "timeout"
                  : "rotate_profile",
              ...(assistantFailoverReason ? { reason: assistantFailoverReason } : {}),
              stage: "assistant",
            });
            if (assistantFailoverOutcome.retryKind === "same_model_idle_timeout") {
              sameModelIdleTimeoutRetries += 1;
            }
            lastRetryFailoverReason = assistantFailoverOutcome.lastRetryFailoverReason;
            continue;
          }
          if (assistantFailoverOutcome.action === "throw") {
            traceAttempts.push({
              provider: activeErrorContext.provider,
              model: activeErrorContext.model,
              result:
                assistantFailoverReason === "timeout"
                  ? "timeout"
                  : assistantFailoverDecision.action === "fallback_model"
                    ? "fallback_model"
                    : "error",
              ...(assistantFailoverReason ? { reason: assistantFailoverReason } : {}),
              stage: "assistant",
              ...(typeof assistantFailoverOutcome.error.status === "number"
                ? { status: assistantFailoverOutcome.error.status }
                : {}),
            });
            if (assistantFailoverOutcome.error.suspend) {
              void suspendSession({
                cfg: params.config,
                agentDir,
                sessionId: activeSessionId ?? params.sessionId,
                laneId: globalLane,
                reason: resolveSessionSuspensionReason(assistantFailoverOutcome.error.reason),
                failedProvider: assistantFailoverOutcome.error.provider ?? provider,
                failedModel: assistantFailoverOutcome.error.model ?? modelId,
              });
            }
            throw assistantFailoverOutcome.error;
          }
          const usageMeta = buildUsageAgentMetaFields({
            usageAccumulator,
            lastAssistantUsage: sessionLastAssistant?.usage as UsageLike | undefined,
            lastRunPromptUsage,
            lastTurnTotal,
          });
          const reportedModelRef = resolveReportedModelRef({
            provider,
            model: model.id,
            assistant: sessionLastAssistant,
          });
          const agentMeta: EmbeddedPiAgentMeta = {
            sessionId: sessionIdUsed,
            sessionFile: sessionFileUsed,
            provider: reportedModelRef.provider,
            model: reportedModelRef.model,
            contextTokens: ctxInfo.tokens,
            agentHarnessId: attempt.agentHarnessId,
            usage: usageMeta.usage,
            lastCallUsage: usageMeta.lastCallUsage,
            promptTokens: usageMeta.promptTokens,
            compactionCount: autoCompactionCount > 0 ? autoCompactionCount : undefined,
            compactionTokensAfter: lastCompactionTokensAfter,
          };
          const finalAssistantVisibleText = resolveFinalAssistantVisibleText(sessionLastAssistant);
          const finalAssistantRawText = resolveFinalAssistantRawText(sessionLastAssistant);

          const payloads = buildEmbeddedRunPayloads({
            assistantTexts: attempt.assistantTexts,
            toolMetas: attempt.toolMetas,
            lastAssistant: attempt.lastAssistant,
            lastToolError: attempt.lastToolError,
            config: params.config,
            isCronTrigger: params.trigger === "cron",
            sessionKey: params.sessionKey ?? params.sessionId,
            provider: activeErrorContext.provider,
            model: activeErrorContext.model,
            verboseLevel: params.verboseLevel,
            reasoningLevel: params.reasoningLevel,
            thinkingLevel: params.thinkLevel,
            toolResultFormat: resolvedToolResultFormat,
            suppressToolErrorWarnings: params.suppressToolErrorWarnings,
            inlineToolResultsAllowed: false,
            didSendViaMessagingTool: attempt.didSendViaMessagingTool,
            didSendDeterministicApprovalPrompt: attempt.didSendDeterministicApprovalPrompt,
            heartbeatToolResponse: attempt.heartbeatToolResponse,
          });
          const payloadsWithToolMedia = mergeAttemptToolMediaPayloads({
            payloads,
            toolMediaUrls: attempt.toolMediaUrls,
            toolAudioAsVoice: attempt.toolAudioAsVoice,
          });
          const timedOutDuringPrompt =
            timedOut && !timedOutDuringCompaction && !timedOutDuringToolExecution;
          const hasPartialAssistantTextAfterPromptTimeout =
            timedOutDuringPrompt &&
            (attempt.assistantTexts ?? []).some((text) => text.trim().length > 0) &&
            !attempt.clientToolCalls &&
            !attempt.yieldDetected &&
            !attempt.didSendViaMessagingTool &&
            !attempt.didSendDeterministicApprovalPrompt &&
            !attempt.lastToolError &&
            (attempt.toolMetas?.length ?? 0) === 0;
          const attemptToolSummary = buildTraceToolSummary({
            toolMetas: attempt.toolMetas,
            hadFailure: Boolean(attempt.lastToolError),
          });
          const failureSignal = resolveEmbeddedRunFailureSignal({
            trigger: params.trigger,
            lastToolError: attempt.lastToolError,
          });

          // Timeout aborts can leave the run without payloads or with only a
          // partial assistant fragment. Emit an explicit timeout error instead,
          // preserving any tool payloads that succeeded before the timeout.
          if (timedOutDuringPrompt && !hasMessagingToolDeliveryEvidence(attempt)) {
            const timeoutText = idleTimedOut
              ? "The model did not produce a response before the model idle timeout. " +
                "Please try again, or increase `models.providers.<id>.timeoutSeconds` for slow local or self-hosted providers."
              : "Request timed out before a response was generated. " +
                "Please try again, or increase `agents.defaults.timeoutSeconds` in your config.";
            const replayInvalid = resolveReplayInvalidForAttempt(null);
            const livenessState = resolveRunLivenessState({
              payloadCount: hasPartialAssistantTextAfterPromptTimeout ? 0 : payloads.length,
              aborted,
              timedOut,
              attempt,
              incompleteTurnText: null,
            });
            attempt.setTerminalLifecycleMeta?.({
              replayInvalid,
              livenessState,
            });
            return {
              payloads: [
                ...(hasPartialAssistantTextAfterPromptTimeout ? [] : payloadsWithToolMedia || []),
                {
                  text: timeoutText,
                  isError: true,
                },
              ],
              meta: {
                durationMs: Date.now() - started,
                agentMeta,
                aborted,
                systemPromptReport: attempt.systemPromptReport,
                finalPromptText: attempt.finalPromptText,
                finalAssistantVisibleText,
                finalAssistantRawText,
                replayInvalid,
                livenessState,
                toolSummary: attemptToolSummary,
                ...(failureSignal ? { failureSignal } : {}),
                agentHarnessResultClassification: attempt.agentHarnessResultClassification,
              },
              didSendViaMessagingTool: attempt.didSendViaMessagingTool,
              didSendDeterministicApprovalPrompt: attempt.didSendDeterministicApprovalPrompt,
              messagingToolSentTexts: attempt.messagingToolSentTexts,
              messagingToolSentMediaUrls: attempt.messagingToolSentMediaUrls,
              messagingToolSentTargets: attempt.messagingToolSentTargets,
              heartbeatToolResponse: attempt.heartbeatToolResponse,
              successfulCronAdds: attempt.successfulCronAdds,
            };
          }

          const silentToolResultReplyPayload = resolveSilentToolResultReplyPayload({
            isCronTrigger: params.trigger === "cron",
            payloadCount: payloadsWithToolMedia?.length ?? 0,
            aborted,
            timedOut,
            attempt,
          });
          const payloadsForTerminalPath = payloadsWithToolMedia?.length
            ? payloadsWithToolMedia
            : silentToolResultReplyPayload
              ? [silentToolResultReplyPayload]
              : payloadsWithToolMedia;
          const payloadCount = payloadsForTerminalPath?.length ?? 0;
          const emptyAssistantReplyIsSilent = shouldTreatEmptyAssistantReplyAsSilent({
            allowEmptyAssistantReplyAsSilent: params.allowEmptyAssistantReplyAsSilent,
            payloadCount,
            aborted,
            timedOut,
            attempt,
          });
          const nextPlanningOnlyRetryInstruction = emptyAssistantReplyIsSilent
            ? null
            : resolvePlanningOnlyRetryInstruction({
                provider,
                modelId,
                executionContract,
                prompt: params.prompt,
                aborted,
                timedOut,
                attempt,
              });
          const nextReasoningOnlyRetryInstruction = emptyAssistantReplyIsSilent
            ? null
            : resolveReasoningOnlyRetryInstruction({
                provider: activeErrorContext.provider,
                modelId: activeErrorContext.model,
                modelApi: effectiveModel.api,
                executionContract,
                aborted,
                timedOut,
                attempt,
              });
          const nextEmptyResponseRetryInstruction = emptyAssistantReplyIsSilent
            ? null
            : resolveEmptyResponseRetryInstruction({
                provider: activeErrorContext.provider,
                modelId: activeErrorContext.model,
                modelApi: effectiveModel.api,
                executionContract,
                payloadCount,
                aborted,
                timedOut,
                attempt,
              });
          if (
            nextPlanningOnlyRetryInstruction &&
            planningOnlyRetryAttempts < maxPlanningOnlyRetryAttempts
          ) {
            const planningOnlyText = (attempt.assistantTexts ?? []).join("\n\n").trim();
            const planDetails = extractPlanningOnlyPlanDetails(planningOnlyText);
            if (planDetails) {
              emitAgentPlanEvent({
                runId: params.runId,
                ...(params.sessionKey ? { sessionKey: params.sessionKey } : {}),
                data: {
                  phase: "update",
                  title: "Assistant proposed a plan",
                  explanation: planDetails.explanation,
                  steps: planDetails.steps,
                  source: "planning_only_retry",
                },
              });
              void params.onAgentEvent?.({
                stream: "plan",
                data: {
                  phase: "update",
                  title: "Assistant proposed a plan",
                  explanation: planDetails.explanation,
                  steps: planDetails.steps,
                  source: "planning_only_retry",
                },
              });
            }
            planningOnlyRetryAttempts += 1;
            planningOnlyRetryInstruction = nextPlanningOnlyRetryInstruction;
            log.warn(
              `planning-only turn detected: runId=${params.runId} sessionId=${params.sessionId} ` +
                `provider=${provider}/${modelId} contract=${executionContract} configured=${configuredExecutionContract} — retrying ` +
                `${planningOnlyRetryAttempts}/${maxPlanningOnlyRetryAttempts} with act-now steer`,
            );
            continue;
          }
          if (
            !nextPlanningOnlyRetryInstruction &&
            nextReasoningOnlyRetryInstruction &&
            reasoningOnlyRetryAttempts < maxReasoningOnlyRetryAttempts
          ) {
            reasoningOnlyRetryAttempts += 1;
            reasoningOnlyRetryInstruction = nextReasoningOnlyRetryInstruction;
            log.warn(
              `reasoning-only assistant turn detected: runId=${params.runId} sessionId=${params.sessionId} ` +
                `provider=${activeErrorContext.provider}/${activeErrorContext.model} — retrying ${reasoningOnlyRetryAttempts}/${maxReasoningOnlyRetryAttempts} ` +
                `with visible-answer continuation`,
            );
            continue;
          }
          const reasoningOnlyRetriesExhausted =
            !nextPlanningOnlyRetryInstruction &&
            nextReasoningOnlyRetryInstruction &&
            reasoningOnlyRetryAttempts >= maxReasoningOnlyRetryAttempts;
          if (
            !nextPlanningOnlyRetryInstruction &&
            !nextReasoningOnlyRetryInstruction &&
            nextEmptyResponseRetryInstruction &&
            emptyResponseRetryAttempts < maxEmptyResponseRetryAttempts
          ) {
            emptyResponseRetryAttempts += 1;
            emptyResponseRetryInstruction = nextEmptyResponseRetryInstruction;
            log.warn(
              `empty response detected: runId=${params.runId} sessionId=${params.sessionId} ` +
                `provider=${activeErrorContext.provider}/${activeErrorContext.model} — retrying ${emptyResponseRetryAttempts}/${maxEmptyResponseRetryAttempts} ` +
                `with visible-answer continuation`,
            );
            continue;
          }
          const incompleteTurnText = emptyAssistantReplyIsSilent
            ? null
            : resolveIncompleteTurnPayloadText({
                payloadCount,
                aborted,
                timedOut,
                attempt,
              });
          if (
            !emptyAssistantReplyIsSilent &&
            attemptCompactionCount > 0 &&
            payloadCount === 0 &&
            !aborted &&
            !promptError &&
            !timedOut &&
            !attempt.clientToolCalls &&
            !attempt.yieldDetected &&
            !attempt.didSendDeterministicApprovalPrompt &&
            !attempt.lastToolError &&
            !resolveAttemptReplayMetadata(attempt).hadPotentialSideEffects &&
            compactionContinuationRetryAttempts < 1
          ) {
            compactionContinuationRetryAttempts += 1;
            compactionContinuationRetryInstruction = COMPACTION_CONTINUATION_RETRY_INSTRUCTION;
            log.warn(
              `compaction interrupted visible final answer: runId=${params.runId} sessionId=${params.sessionId} ` +
                `compactions=${attemptCompactionCount} — retrying ${compactionContinuationRetryAttempts}/1 with compacted-transcript continuation`,
            );
            postCompactionGuard.armPostCompaction();
            continue;
          }
          compactionContinuationRetryInstruction = null;
          if (reasoningOnlyRetriesExhausted && !finalAssistantVisibleText) {
            log.warn(
              `reasoning-only retries exhausted: runId=${params.runId} sessionId=${params.sessionId} ` +
                `provider=${activeErrorContext.provider}/${activeErrorContext.model} attempts=${reasoningOnlyRetryAttempts}/${maxReasoningOnlyRetryAttempts} — surfacing incomplete-turn error`,
            );
          }
          if (!incompleteTurnText && nextPlanningOnlyRetryInstruction && strictAgenticActive) {
            log.warn(
              `strict-agentic run exhausted planning-only retries: runId=${params.runId} sessionId=${params.sessionId} ` +
                `provider=${provider}/${modelId} configured=${configuredExecutionContract} — surfacing blocked state`,
            );
            // Criterion 4 of the GPT-5.4 parity gate requires every terminal
            // exit path to emit an explicit livenessState + replayInvalid so
            // downstream observers never see "silent disappearance". Every
            // other hard-error terminal branch in this file uses "blocked"
            // for its livenessState (role ordering, image size, schema
            // error, compaction timeout, aborted-with-no-payloads). Match
            // that convention here so lifecycle consumers treat an
            // isError:true strict-agentic-blocked payload the same way they
            // treat any other error-terminal payload. Replay validity is
            // delegated to the shared resolver because the plan-only
            // transcript itself is replay-safe even though the run is
            // terminal.
            const replayInvalid = resolveReplayInvalidForAttempt(null);
            const livenessState: EmbeddedRunLivenessState = "blocked";
            attempt.setTerminalLifecycleMeta?.({
              replayInvalid,
              livenessState,
            });
            return {
              payloads: [
                {
                  text: STRICT_AGENTIC_BLOCKED_TEXT,
                  isError: true,
                },
              ],
              meta: {
                durationMs: Date.now() - started,
                agentMeta,
                aborted,
                systemPromptReport: attempt.systemPromptReport,
                finalPromptText: attempt.finalPromptText,
                finalAssistantVisibleText,
                finalAssistantRawText,
                replayInvalid,
                livenessState,
                toolSummary: attemptToolSummary,
                ...(failureSignal ? { failureSignal } : {}),
                agentHarnessResultClassification: attempt.agentHarnessResultClassification,
              },
              didSendViaMessagingTool: attempt.didSendViaMessagingTool,
              didSendDeterministicApprovalPrompt: attempt.didSendDeterministicApprovalPrompt,
              messagingToolSentTexts: attempt.messagingToolSentTexts,
              messagingToolSentMediaUrls: attempt.messagingToolSentMediaUrls,
              messagingToolSentTargets: attempt.messagingToolSentTargets,
              heartbeatToolResponse: attempt.heartbeatToolResponse,
              successfulCronAdds: attempt.successfulCronAdds,
            };
          }
          if (reasoningOnlyRetriesExhausted && !finalAssistantVisibleText) {
            const replayInvalid = resolveReplayInvalidForAttempt(
              "⚠️ Agent couldn't generate a response. Please try again.",
            );
            const livenessState = resolveRunLivenessState({
              payloadCount: 0,
              aborted,
              timedOut,
              attempt,
              incompleteTurnText: "⚠️ Agent couldn't generate a response. Please try again.",
            });
            attempt.setTerminalLifecycleMeta?.({
              replayInvalid,
              livenessState,
            });
            if (lastProfileId) {
              await maybeMarkAuthProfileFailure({
                profileId: lastProfileId,
                reason: resolveRunAuthProfileFailureReason(assistantFailoverReason),
              });
            }
            return {
              payloads: [
                {
                  text: "⚠️ Agent couldn't generate a response. Please try again.",
                  isError: true,
                },
              ],
              meta: {
                durationMs: Date.now() - started,
                agentMeta,
                aborted,
                systemPromptReport: attempt.systemPromptReport,
                finalPromptText: attempt.finalPromptText,
                finalAssistantVisibleText,
                finalAssistantRawText,
                replayInvalid,
                livenessState,
                toolSummary: attemptToolSummary,
                ...(failureSignal ? { failureSignal } : {}),
                agentHarnessResultClassification: attempt.agentHarnessResultClassification,
              },
              didSendViaMessagingTool: attempt.didSendViaMessagingTool,
              didSendDeterministicApprovalPrompt: attempt.didSendDeterministicApprovalPrompt,
              messagingToolSentTexts: attempt.messagingToolSentTexts,
              messagingToolSentMediaUrls: attempt.messagingToolSentMediaUrls,
              messagingToolSentTargets: attempt.messagingToolSentTargets,
              heartbeatToolResponse: attempt.heartbeatToolResponse,
              successfulCronAdds: attempt.successfulCronAdds,
            };
          }
          if (
            !nextPlanningOnlyRetryInstruction &&
            !nextReasoningOnlyRetryInstruction &&
            nextEmptyResponseRetryInstruction &&
            emptyResponseRetryAttempts >= maxEmptyResponseRetryAttempts
          ) {
            log.warn(
              `empty response retries exhausted: runId=${params.runId} sessionId=${params.sessionId} ` +
                `provider=${activeErrorContext.provider}/${activeErrorContext.model} attempts=${emptyResponseRetryAttempts}/${maxEmptyResponseRetryAttempts} — surfacing incomplete-turn error`,
            );
          }
          // ── silent-error retry ────────────────────────────────────────────
          // Observed with ollama/glm-5.1: a turn can end with stopReason="error"
          // and zero output tokens AND empty content after a successful
          // tool-call sequence, producing no user-visible text at all. This
          // path is narrower than the empty-response continuation retry:
          // same prompt, same session transcript (tool results already
          // captured), no instruction injection. Placed before the
          // incompleteTurnText return so it actually gets a chance to fire.
          //
          // Content-empty guard: a reasoning-only error (content has thinking
          // blocks) is a distinct failure mode handled elsewhere; only retry
          // when the assistant truly produced nothing.
          //
          // Side-effect guard: if the failed attempt already recorded potential
          // side effects (messaging tool sent, cron add, mutating tool
          // call that wasn't round-tripped as replay-safe), resubmission can
          // duplicate those actions. Mirror the gate the other retry resolvers
          // use (resolveEmptyResponseRetryInstruction, reasoning-only, planning-
          // only), which short-circuit on attempt.replayMetadata.hadPotentialSideEffects.
          const silentErrorContent = sessionLastAssistant?.content as Array<unknown> | undefined;
          if (
            incompleteTurnText &&
            !aborted &&
            !promptError &&
            !timedOut &&
            sessionLastAssistant?.stopReason === "error" &&
            ((sessionLastAssistant?.usage as { output?: number } | undefined)?.output ?? 0) === 0 &&
            (silentErrorContent?.length ?? 0) === 0 &&
            (attempt.replayMetadata ? !attempt.replayMetadata.hadPotentialSideEffects : false) &&
            emptyErrorRetries < MAX_EMPTY_ERROR_RETRIES
          ) {
            emptyErrorRetries += 1;
            log.warn(
              `[empty-error-retry] stopReason=error output=0; resubmitting ` +
                `attempt=${emptyErrorRetries}/${MAX_EMPTY_ERROR_RETRIES} ` +
                `provider=${sessionLastAssistant?.provider ?? provider} ` +
                `model=${sessionLastAssistant?.model ?? model.id} ` +
                `sessionKey=${params.sessionKey ?? params.sessionId}`,
            );
            continue;
          }
          if (incompleteTurnText) {
            const replayInvalid = resolveReplayInvalidForAttempt(incompleteTurnText);
            const livenessState = resolveRunLivenessState({
              payloadCount,
              aborted,
              timedOut,
              attempt,
              incompleteTurnText,
            });
            attempt.setTerminalLifecycleMeta?.({
              replayInvalid,
              livenessState,
            });
            const incompleteStopReason = attempt.lastAssistant?.stopReason;
            log.warn(
              `incomplete turn detected: runId=${params.runId} sessionId=${params.sessionId} ` +
                `stopReason=${incompleteStopReason} payloads=${payloadCount} — surfacing error to user`,
            );

            // Mark the failing profile for cooldown so multi-profile setups
            // rotate away from the exhausted credential on the next turn.
            if (lastProfileId) {
              await maybeMarkAuthProfileFailure({
                profileId: lastProfileId,
                reason: resolveRunAuthProfileFailureReason(assistantFailoverReason),
              });
            }

            return {
              payloads: [
                {
                  text: incompleteTurnText,
                  isError: true,
                },
              ],
              meta: {
                durationMs: Date.now() - started,
                agentMeta,
                aborted,
                systemPromptReport: attempt.systemPromptReport,
                finalPromptText: attempt.finalPromptText,
                finalAssistantVisibleText,
                finalAssistantRawText,
                replayInvalid,
                livenessState,
                toolSummary: attemptToolSummary,
                ...(failureSignal ? { failureSignal } : {}),
                agentHarnessResultClassification: attempt.agentHarnessResultClassification,
              },
              didSendViaMessagingTool: attempt.didSendViaMessagingTool,
              didSendDeterministicApprovalPrompt: attempt.didSendDeterministicApprovalPrompt,
              messagingToolSentTexts: attempt.messagingToolSentTexts,
              messagingToolSentMediaUrls: attempt.messagingToolSentMediaUrls,
              messagingToolSentTargets: attempt.messagingToolSentTargets,
              heartbeatToolResponse: attempt.heartbeatToolResponse,
              successfulCronAdds: attempt.successfulCronAdds,
            };
          }

          log.debug(
            `embedded run done: runId=${params.runId} sessionId=${params.sessionId} durationMs=${Date.now() - started} aborted=${aborted}`,
          );
          if (lastProfileId) {
            await markAuthProfileSuccess({
              store: profileFailureStore,
              provider: resolveAuthProfileStateProvider(
                profileFailureStore,
                lastProfileId,
                provider,
              ),
              profileId: lastProfileId,
              agentDir: params.agentDir,
            });
          }
          const replayInvalid = resolveReplayInvalidForAttempt(null);
          const livenessState = attempt.yieldDetected
            ? "paused"
            : resolveRunLivenessState({
                payloadCount,
                aborted,
                timedOut,
                attempt,
                incompleteTurnText: null,
              });
          const stopReason = attempt.clientToolCalls
            ? "tool_calls"
            : attempt.yieldDetected
              ? "end_turn"
              : (sessionLastAssistant?.stopReason as string | undefined);
          const terminalPayloads = emptyAssistantReplyIsSilent
            ? [{ text: SILENT_REPLY_TOKEN }]
            : payloadsForTerminalPath;
          attempt.setTerminalLifecycleMeta?.({
            replayInvalid,
            livenessState,
            stopReason,
            yielded: attempt.yieldDetected === true,
          });
          return {
            payloads: terminalPayloads?.length ? terminalPayloads : undefined,
            ...(attempt.diagnosticTrace
              ? { diagnosticTrace: freezeDiagnosticTraceContext(attempt.diagnosticTrace) }
              : {}),
            meta: {
              durationMs: Date.now() - started,
              agentMeta,
              aborted,
              systemPromptReport: attempt.systemPromptReport,
              finalPromptText: attempt.finalPromptText,
              finalAssistantVisibleText,
              finalAssistantRawText,
              replayInvalid,
              livenessState,
              agentHarnessResultClassification: attempt.agentHarnessResultClassification,
              ...(attempt.yieldDetected ? { yielded: true } : {}),
              ...(emptyAssistantReplyIsSilent
                ? { terminalReplyKind: "silent-empty" as const }
                : {}),
              // Handle client tool calls (OpenResponses hosted tools)
              // Propagate the LLM stop reason so callers (lifecycle events,
              // ACP bridge) can distinguish end_turn from max_tokens.
              stopReason,
              pendingToolCalls: attempt.clientToolCalls?.map((call) => ({
                id: randomBytes(5).toString("hex").slice(0, 9),
                name: call.name,
                arguments: JSON.stringify(call.params),
              })),
              executionTrace: {
                winnerProvider: reportedModelRef.provider,
                winnerModel: reportedModelRef.model,
                attempts:
                  traceAttempts.length > 0 ||
                  sessionLastAssistant?.provider ||
                  sessionLastAssistant?.model
                    ? [
                        ...traceAttempts,
                        {
                          provider: reportedModelRef.provider,
                          model: reportedModelRef.model,
                          result: "success",
                          stage: "assistant",
                        },
                      ]
                    : undefined,
                fallbackUsed: traceAttempts.length > 0,
                runner: "embedded",
              },
              requestShaping: {
                ...(lastProfileId ? { authMode: "auth-profile" } : {}),
                ...(thinkLevel ? { thinking: thinkLevel } : {}),
                ...(params.reasoningLevel ? { reasoning: params.reasoningLevel } : {}),
                ...(params.verboseLevel ? { verbose: params.verboseLevel } : {}),
                ...(params.blockReplyBreak ? { blockStreaming: params.blockReplyBreak } : {}),
              },
              toolSummary: attemptToolSummary,
              ...(failureSignal ? { failureSignal } : {}),
              completion: {
                ...(stopReason ? { stopReason } : {}),
                ...(stopReason ? { finishReason: stopReason } : {}),
                ...(stopReason?.toLowerCase().includes("refusal") ? { refusal: true } : {}),
              },
              contextManagement:
                autoCompactionCount > 0 ? { lastTurnCompactions: autoCompactionCount } : undefined,
            },
            didSendViaMessagingTool: attempt.didSendViaMessagingTool,
            didSendDeterministicApprovalPrompt: attempt.didSendDeterministicApprovalPrompt,
            messagingToolSentTexts: attempt.messagingToolSentTexts,
            messagingToolSentMediaUrls: attempt.messagingToolSentMediaUrls,
            messagingToolSentTargets: attempt.messagingToolSentTargets,
            heartbeatToolResponse: attempt.heartbeatToolResponse,
            successfulCronAdds: attempt.successfulCronAdds,
          };
        }
      } finally {
        forgetPromptBuildDrainCacheForRun(params.runId);
        stopRuntimeAuthRefreshTimer();
        await runAgentCleanupStep({
          runId: params.runId,
          sessionId: params.sessionId,
          step: "context-engine-dispose",
          log,
          cleanup: async () => {
            await contextEngine.dispose?.();
          },
        });
        if (params.cleanupBundleMcpOnRunEnd === true) {
          await runAgentCleanupStep({
            runId: params.runId,
            sessionId: params.sessionId,
            step: "bundle-mcp-retire",
            log,
            cleanup: async () => {
              const onError = (error: unknown, sessionId: string) => {
                log.warn(
                  `bundle-mcp cleanup failed after run for ${sessionId}: ${formatErrorMessage(error)}`,
                );
              };
              const retiredBySessionKey = await retireSessionMcpRuntimeForSessionKey({
                sessionKey: params.sessionKey,
                reason: "embedded-run-end",
                onError,
              });
              if (!retiredBySessionKey) {
                await retireSessionMcpRuntime({
                  sessionId: params.sessionId,
                  reason: "embedded-run-end",
                  onError,
                });
              }
            },
          });
        }
      }
    });
  });
}

//按优先级安全获取 AI 服务商名称：配置里的 provider → profileId 冒号前的前缀 → 默认值，永远返回有效字符串。
function resolveAuthProfileStateProvider(
  store: AuthProfileStore,    // 认证配置仓库（存所有 API Key）
  profileId: string,          // 当前要查询的配置 ID
  fallbackProvider: string,   
): string {
  const profileProvider = store.profiles?.[profileId]?.provider?.trim();
  if (profileProvider) {
    return profileProvider;
  }
  const idProvider = profileId.split(":", 1)[0]?.trim();
  return idProvider || fallbackProvider;
}
