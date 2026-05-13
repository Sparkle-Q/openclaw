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

// 异步函数：运行嵌入式 Pi 智能代理
export async function runEmbeddedPiAgent(
  params: RunEmbeddedPiAgentParams,   // 入参：所有运行需要的配置、会话、代理信息
): Promise<EmbeddedPiRunResult> {    // 返回值：异步返回运行结果
  // Resolve sessionKey early so all downstream consumers (hooks, LCM, compaction)
  // receive a non-null key even when callers omit it. See #60552.
  // 提前解析 sessionKey，确保下游所有模块（钩子、会话管理、上下文压缩）都能拿到合法的Key
  // 即使调用方没传，也能拿到非空的 key。对应 GitHub Issue #60552
  const effectiveSessionKey = backfillSessionKey({
    config: params.config,
    sessionId: params.sessionId,
    sessionKey: params.sessionKey,
    agentId: params.agentId,
  });

  // 如果解析后的 key 和原来传入的不一样（说明自动回填/修正了）
  if (effectiveSessionKey !== params.sessionKey) {
    // 用新的 sessionKey 替换原来的参数
    // 创建新对象，不修改原始参数（无副作用）
    params = { ...params, sessionKey: effectiveSessionKey };
  }
  // 用户隔离队列,每个用户一个独立队列，防止任务互相干扰.根据 sessionKey / sessionId 生成
  const sessionLane = resolveSessionLane(params.sessionKey?.trim() || params.sessionId);
  // 公共全局队列,所有用户共享的通道
  const globalLane = resolveGlobalLane(params.lane);
  // 计算任务超时, 自动 +30 秒宽限期
  const laneTaskTimeoutMs = resolveEmbeddedRunLaneTimeoutMs(params.timeoutMs);
  // 给任务自动加上超时
  const withLaneTimeout = (opts?: CommandQueueEnqueueOptions) =>
    withEmbeddedRunLaneTimeout(opts, laneTaskTimeoutMs);
  // 全局任务执行,任务进入公共队列,自动带超时
  const enqueueGlobal = <T>(task: () => Promise<T>, opts?: CommandQueueEnqueueOptions) =>
    params.enqueue
      ? params.enqueue(task, withLaneTimeout(opts))
      : enqueueCommandInLane(globalLane, task, withLaneTimeout(opts));
  // 会话任务执行, 任务进入用户独立队列, 确保用户之间完全隔离
  const enqueueSession = <T>(task: () => Promise<T>, opts?: CommandQueueEnqueueOptions) =>
    params.enqueue ? params.enqueue(task, opts) : enqueueCommandInLane(sessionLane, task, opts);
  // 消息渠道（如：短信、钉钉、飞书、Web、微信等）
  const channelHint = params.messageChannel ?? params.messageProvider;
  // 自动判断返回格式：支持 Markdown → markdown,不支持（如短信）→ plain 纯文本. 默认 markdown
  const resolvedToolResultFormat =
    params.toolResultFormat ??
    (channelHint
      ? isMarkdownCapableMessageChannel(channelHint)
        ? "markdown"
        : "plain"
      : "markdown");
  // 是否是测试探测会话, sessionId 以 probe- 开头就是测试
  const isProbeSession = params.sessionId?.startsWith("probe-") ?? false;

  // 检查任务是否被取消，如果已取消，立即抛出 AbortError 中断执行
  const throwIfAborted = () => {
    // 1. 如果没有取消信号 或 任务未取消 → 直接返回，继续执行
    if (!params.abortSignal?.aborted) {
      return;
    }
    // 2. 代码走到这里 = 任务已经被取消（aborted）
    const reason = params.abortSignal.reason;  // 获取取消原因
    
    // 3. 如果原因本身就是 Error 对象 → 直接抛出
    if (reason instanceof Error) {
      throw reason;
    }
    // 4. 否则 → 创建标准的 AbortError 对象
    const abortErr =
      reason !== undefined
        ? new Error("Operation aborted", { cause: reason })  // 带原因
        : new Error("Operation aborted");   // 不带原因
    
    // 标准错误名称，方便上层识别是“取消”而非普通报错
    abortErr.name = "AbortError";
    // 5. 抛出错误，终止任务
    throw abortErr;
  };

  throwIfAborted();

  // 最外层：把整个任务丢进【用户会话隔离队列】执行
  return enqueueSession(() => {
    // 立刻检查：任务是否已被取消（用户取消/超时）
    throwIfAborted();
    
    // 内层：再把核心执行逻辑丢进【全局公共队列】
    return enqueueGlobal(async () => {
      // 再次检查取消（双重保险）
      throwIfAborted();
      // 记录任务开始时间（用于统计耗时）
      const started = Date.now();
      // 创建任务启动阶段追踪器（跟踪：初始化/加载模型/调用工具等阶段）
      const startupStages = createEmbeddedRunStageTracker();
      // 标记：启动阶段事件是否已发送
      let startupStagesEmitted = false;
      
      // 定义一个【执行阶段通知工具函数】
      // 作用：向外发送当前AI任务跑到哪个阶段了（如：思考中、调用工具、生成答案）
      const notifyExecutionPhase = (
        phase: Parameters<NonNullable<RunEmbeddedPiAgentParams["onExecutionPhase"]>>[0]["phase"],  //阶段类型
        extra?: Omit<   // 额外信息
          Parameters<NonNullable<RunEmbeddedPiAgentParams["onExecutionPhase"]>>[0],
          "phase"
        >,
      ) => {
        // 如果外部传入了回调函数，就调用它，通知外部当前阶段
        params.onExecutionPhase?.({ phase, ...extra });
      };
      
      // 输出【启动阶段耗时总结日志】
      // phase：当前处于哪个阶段（如：started / model_called / completed）
      const emitStartupStageSummary = (phase: string) => {
        // 1. 获取阶段追踪器的快照（所有阶段的耗时、步骤）
        const summary = startupStages.snapshot();
        // 2. 判断是否需要警告（某些阶段过慢 → 警告日志）
        const shouldWarn = shouldWarnEmbeddedRunStageSummary(summary);
        // 3. 优化：不需要警告 + 没开 trace 日志 → 直接跳过，不打印
        if (!shouldWarn && !log.isEnabled("trace")) {
          return;
        }

        // 4. 格式化日志消息（包含 runId、sessionId、阶段、耗时详情）
        const message = formatEmbeddedRunStageSummary(
          `[trace:embedded-run] startup stages: runId=${params.runId} sessionId=${params.sessionId} phase=${phase}`,
          summary,
        );

        // 5. 根据是否需要警告，决定用 warn 还是 trace 级别打印
        if (shouldWarn) {
          log.warn(message);  // 慢/异常 → 警告级别
        } else {
          log.trace(message);   // 正常 → 调试/追踪级别
        }
      };
      
      // 1. 触发外部回调：告诉上层“任务正式开始执行了”
      params.onExecutionStarted?.();
      // 2. 通知执行阶段：当前进入【运行器已启动】阶段
      notifyExecutionPhase("runner_entered");
      // 3. 解析本次运行的工作目录（存放临时文件、缓存、工具生成的文件）
      const workspaceResolution = resolveRunWorkspaceDir({
        workspaceDir: params.workspaceDir,
        sessionKey: params.sessionKey,
        agentId: params.agentId,
        config: params.config,
      });
      
      // 4. 最终确定的工作目录路径
      const resolvedWorkspace = workspaceResolution.workspaceDir;
      // 5. 解析“标准/官方”工作目录（系统默认给这个代理分配的目录）
      const canonicalWorkspace = resolveUserPath(
        resolveAgentWorkspaceDir(params.config ?? {}, workspaceResolution.agentId),
      );
      // 6. 判断：当前目录 是否等于 系统标准目录
      const isCanonicalWorkspace = canonicalWorkspace === resolvedWorkspace;
      // 7. 安全脱敏：给 sessionId 打码（日志不泄露真实ID）
      const redactedSessionId = redactRunIdentifier(params.sessionId);
      // 8. 安全脱敏：给 sessionKey 打码
      const redactedSessionKey = redactRunIdentifier(params.sessionKey);
      // 9. 安全脱敏：给工作目录路径打码
      const redactedWorkspace = redactRunIdentifier(resolvedWorkspace);
      // 10. 如果工作目录使用了【降级/兜底 fallback】路径 → 打印警告日志
      if (workspaceResolution.usedFallback) {
        log.warn(
          `[workspace-fallback] caller=runEmbeddedPiAgent reason=${workspaceResolution.fallbackReason} run=${params.runId} session=${redactedSessionId} sessionKey=${redactedSessionKey} agent=${workspaceResolution.agentId} workspace=${redactedWorkspace}`,
        );
      }
      // 11. 标记阶段：工作目录准备完成（耗时追踪）
      startupStages.mark("workspace");
      // 12. 通知外部：当前进入 workspace 准备完成阶段
      notifyExecutionPhase("workspace");
      // 13. 确保运行时插件全部加载完成（核心功能扩展）
      ensureRuntimePluginsLoaded({
        config: params.config,
        workspaceDir: resolvedWorkspace,
        allowGatewaySubagentBinding: params.allowGatewaySubagentBinding,
      });
      // 14. 标记阶段：运行时插件加载完成
      startupStages.mark("runtime-plugins");
      // 15. 通知外部：进入插件加载完成阶段
      notifyExecutionPhase("runtime_plugins");

      let provider = (params.provider ?? DEFAULT_PROVIDER).trim() || DEFAULT_PROVIDER;
      let modelId = (params.model ?? DEFAULT_MODEL).trim() || DEFAULT_MODEL;
      // agent的本地配置目录，存放：模型配置、工具、插件、脚本
      const agentDir =
        params.agentDir ?? resolveAgentDir(params.config ?? {}, workspaceResolution.agentId);
      const normalizedSessionKey = params.sessionKey?.trim();
      // 判断是否配置了模型自动降级
      const fallbackConfigured = hasConfiguredModelFallbacks({
        cfg: params.config,
        agentId: params.agentId,
        sessionKey: normalizedSessionKey,
      });
      const resolvedSessionKey = normalizedSessionKey;
      // 全局钩子执行器，负责运行beforeRun/afterRun/...等生命周期函数
      const hookRunner = getGlobalHookRunner();
      // 钩子上下文 = 任务的“身份证+全部信息”
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
      
      // 定时任务（cron）专属钩子逻辑，
      // 作用是：在定时任务触发时，提前运行回复钩子，如果钩子直接处理了请求，就直接返回结果，不再走完整 AI 流程。
      if (params.trigger === "cron" && hookRunner?.hasHooks("before_agent_reply")) {  // 1. 判断：只有【定时任务 cron 触发】 + 【注册了 before_agent_reply 钩子】才执行
        // 2. 执行【回复前钩子】，传入用户提示词 + 上下文
        const hookResult = await hookRunner.runBeforeAgentReply(
          { cleanedBody: params.prompt },  // 输入：用户的提示词
          hookCtx,       // 上下文：任务全部信息
        );
        // 3. 如果钩子返回 handled = true → 表示【已由钩子直接处理，不用AI了】
        if (hookResult?.handled) {
          // 4. 直接构造返回结果，提前结束任务
          return {
            // 标准化回复 payload
            payloads: buildHandledReplyPayloads(hookResult.reply),
            // 元信息（耗时、模型、会话等）
            meta: {
              durationMs: Date.now() - started,              // 总耗时
              agentMeta: {
                sessionId: params.sessionId,                 // 会话ID
                provider,                                    // 服务商
                model: modelId,                              // 模型
              },
              // 最终回复文本
              finalAssistantVisibleText: hookResult.reply?.text ?? SILENT_REPLY_TOKEN,
              finalAssistantRawText: hookResult.reply?.text ?? SILENT_REPLY_TOKEN,
            },
          };
        }
      }
      // 通过钩子动态修改最终要使用的 AI 服务商和模型 
      const hookSelection = await resolveHookModelSelection({
        prompt: params.prompt,              // 用户输入的提示词
        attachments: buildBeforeModelResolveAttachments(params.images),   // 图片/附件
        provider,     // 当前默认的服务商
        modelId,      // 当前默认的模型ID
        hookRunner,   // 钩子运行器
        hookContext: hookCtx,   // 钩子上下文（任务全部信息）
      });
      // 用【钩子返回的结果】覆盖原来的服务商
      provider = hookSelection.provider;
      // 用【钩子返回的结果】覆盖原来的模型ID
      modelId = hookSelection.modelId;
      // 兼容旧版钩子的返回结果（历史遗留兼容）
      const legacyBeforeAgentStartResult = hookSelection.legacyBeforeAgentStartResult;
      // 性能打点：标记钩子执行完成
      startupStages.mark("hooks");
      // 确保【当前选中的模型/服务商】对应的插件已经加载完毕
      await ensureSelectedAgentHarnessPlugin({
        provider,
        modelId,
        config: params.config,
        agentId: params.agentId,
        sessionKey: params.sessionKey,
        workspaceDir: resolvedWorkspace,
      });
      // 根据模型、服务商、配置 → 选中【真正负责执行AI的执行器】
      const agentHarness = selectAgentHarness({
        provider,
        modelId,
        config: params.config,
        agentId: params.agentId,
        sessionKey: params.sessionKey,
        agentHarnessId: params.agentHarnessId,
      });
      // 判断：插件执行器是否自己掌管消息传输（发送/接收） 
      const pluginHarnessOwnsTransport = agentHarness.id !== "pi";
      // 动态解析最终要调用的真实模型（核心！）
      const dynamicModelResolution = await resolveModelAsync(
        provider,
        modelId,
        agentDir,
        params.config,
        {
          // Plugin dynamic model hooks can resolve explicit model refs without
          // first generating PI models.json. This keeps one-shot model runs from
          // blocking on unrelated provider discovery.
          // 跳过自动模型发现，提高一次性执行速度
          skipPiDiscovery: true,
          workspaceDir: resolvedWorkspace,
        },
      );
      // 最终确定模型解析结果（二选一逻辑）
      const modelResolution =
        // 如果动态解析已经拿到有效模型  OR  插件自己管传输
        dynamicModelResolution.model || pluginHarnessOwnsTransport
          ? dynamicModelResolution   // 情况 A：直接用动态解析的结果（最快）
          : await (async () => {   // 情况 B：没拿到模型，必须走完整流程加载模型配置
              await ensureOpenClawModelsJson(params.config, agentDir, {   // 确保系统模型配置文件 models.json 已生成/存在
                workspaceDir: resolvedWorkspace,
              });
              // 再次执行完整的模型解析
              return await resolveModelAsync(provider, modelId, agentDir, params.config, {
                workspaceDir: resolvedWorkspace,
              });
            })();
      // 从最终结果里解构出 4 个核心东西
      const { model, error, authStorage, modelRegistry } = modelResolution;
      // 检查模型是否存在 → 不存在就抛专门的降级错误
      if (!model) {
        throw new FailoverError(error ?? `Unknown model: ${provider}/${modelId}`, {
          reason: "model_not_found",
          provider,
          model: modelId,
          sessionId: params.sessionId,
          lane: globalLane,
        });
      }
      // 存在就把最终确定的真实模型存起来
      // 后面真正调用 AI、发请求、推理都用这个变量
      let runtimeModel = model;

      // 解析【最终生效的运行时模型】（核心：合并配置、调整参数、确定上下文窗口大小）
      const resolvedRuntimeModel = resolveEffectiveRuntimeModel({
        cfg: params.config,
        provider,
        contextConfigProvider: resolveHarnessContextConfigProvider({
          provider,
          harnessId: agentHarness.id,
        }),
        modelId,
        runtimeModel,    // 上一步确定的模型对象
      });
      // 取出上下文信息（最大token数、窗口大小等）
      const ctxInfo = resolvedRuntimeModel.ctxInfo;
      // 最终确定、真正用于调用的模型
      let effectiveModel = resolvedRuntimeModel.effectiveModel;
      // 性能打点：模型解析阶段完成
      startupStages.mark("model-resolution");
      // 通知外部：进入【模型解析完成】阶段
      notifyExecutionPhase("model_resolution", { provider, model: modelId });

      // 创建【认证存储】（存放 API Key、配置、权限）
      const authStore = pluginHarnessOwnsTransport
        ? createEmptyAuthProfileStore()  // 如果是插件自己管消息 → 用空的认证存储（不需要系统提供密钥）
        : ensureAuthProfileStoreWithoutExternalProfiles(agentDir, {  // 否则 → 加载系统正常的认证存储（读取本地的 API Key）
            allowKeychainPrompt: false,   // 不弹出密钥提示
          });
      // 用于【尝试认证】的存储（备用/兜底）
      const attemptAuthProfileStore = pluginHarnessOwnsTransport
        ? ensureAuthProfileStoreWithoutExternalProfiles(agentDir, {   // 插件模式：必须用真实存储去尝试调用
            allowKeychainPrompt: false,
          })
        : authStore;  // 普通模式：直接用上面的主存储
      // 用户/调用方 请求使用的认证配置 ID
      const requestedProfileId = params.authProfileId?.trim();
      // 标记：这个认证配置是不是【用户手动指定的】（锁定不可自动替换）
      const requestedProfileIsUserLocked = params.authProfileIdSource === "user";
      // 判断插件是否可以【转发/使用】这个认证配置
      const isForwardablePluginHarnessAuthProfile = (
        profileId: string | undefined,
      ): profileId is string => {
        // 1. 如果不是插件模式 或 没有 profileId → 直接不能转发
        if (!pluginHarnessOwnsTransport || !profileId) {
          return false;
        }
        // 2. 从认证存储里获取这个 ID 对应的凭证
        const credential = attemptAuthProfileStore.profiles?.[profileId];
        // 3. 构建【运行时认证计划】——计算这个认证应该怎么用
        const runtimeAuthPlan = buildAgentRuntimeAuthPlan({
          provider,
          // 优先用凭证里的 provider，没有就从 profileId 截取前缀
          authProfileProvider: credential?.provider ?? profileId.split(":", 1)[0],
          authProfileMode: credential?.type,
          sessionAuthProfileId: profileId,
          config: params.config,
          workspaceDir: resolvedWorkspace,
          harnessId: agentHarness.id,
          harnessRuntime: agentHarness.id,
          allowHarnessAuthProfileForwarding: true,  // 允许插件转发认证
        });
        // 最终判断：计划转发的 ID 是不是等于传入的 ID
        // 相等 = 安全、合法、可以转发
        return runtimeAuthPlan.forwardedAuthProfileId === profileId;
      };
      // 给插件模式生成一个「可安全使用的认证配置优先级列表」，告诉系统按什么顺序尝试 API Key。
      const resolvePluginHarnessProfileOrder = (): string[] => {
        // 1. 如果用户手动指定了认证ID，并且锁定不可修改
        if (requestedProfileId && requestedProfileIsUserLocked) {
          // 检查这个ID是否允许插件转发
          return isForwardablePluginHarnessAuthProfile(requestedProfileId)
            ? [requestedProfileId]   // 允许 → 只用这一个
            : [];                    // 不允许 → 空列表，不能用
        }
        // 2. 如果不是插件自己管传输 → 不需要插件认证列表
        if (!pluginHarnessOwnsTransport) {
          return [];
        }
        // 3. 构建运行时认证计划，获取插件支持的服务商
        const runtimeAuthPlan = buildAgentRuntimeAuthPlan({
          provider,
          config: params.config,
          workspaceDir: resolvedWorkspace,
          harnessId: agentHarness.id,
          harnessRuntime: agentHarness.id,
          allowHarnessAuthProfileForwarding: true,
        });
        const harnessAuthProvider = runtimeAuthPlan.harnessAuthProvider;
        // 4. 插件没有指定支持的服务商 → 无法提供列表
        if (!harnessAuthProvider) {
          return [];
        }
        // 5. 解析出当前服务商下所有可用的认证，并过滤出【允许插件转发】的
        const resolvedOrder = resolveAuthProfileOrder({
          cfg: params.config,
          store: attemptAuthProfileStore,
          provider: harnessAuthProvider,
        }).filter(isForwardablePluginHarnessAuthProfile);
        // 6. 有可用的 → 返回排序后的列表
        if (resolvedOrder.length > 0) {
          return resolvedOrder;
        }
        // 7. 兜底：如果上面没找到，再看用户指定的是否可用
        if (requestedProfileId && isForwardablePluginHarnessAuthProfile(requestedProfileId)) {
          return [requestedProfileId];
        }
        // 8. 全都不可用 → 空列表
        return [];
      };
      // 如果是插件自己管传输 → 获取可用认证列表，否则空数组
      const pluginHarnessProfileOrder = pluginHarnessOwnsTransport
        ? resolvePluginHarnessProfileOrder()
        : [];
      // 取列表里第一个（优先级最高的认证）
      const resolvePluginHarnessPreferredProfileId = (): string | undefined =>
        pluginHarnessProfileOrder[0];
      // 最终优先使用的认证ID
      // 插件模式 → 用上面算出的首选
      // 普通模式 → 用用户请求的
      const preferredProfileId = pluginHarnessOwnsTransport
        ? resolvePluginHarnessPreferredProfileId()
        : requestedProfileId;
      // 初始锁定认证ID：只有用户手动指定的才锁定
      let lockedProfileId = requestedProfileIsUserLocked ? preferredProfileId : undefined;
      // 如果有锁定ID → 校验合法性，不合法就清空（不锁定）
      if (lockedProfileId) {
        if (pluginHarnessOwnsTransport) {
          // 插件模式：必须是可转发的才保留，否则取消锁定
          if (!isForwardablePluginHarnessAuthProfile(lockedProfileId)) {
            lockedProfileId = undefined;
          }
        } else {
          // 普通模式：检查锁定的认证 服务商 是否和当前运行的服务商一致
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
          // 服务商不匹配 → 取消锁定
          if (!lockedProfile || !lockedProfileProvider || lockedProfileProvider !== runProvider) {
            lockedProfileId = undefined;
          }
        }
      }
      // 插件模式：最终要【转发给插件】的认证ID
      const forwardedPluginHarnessProfileId =
        pluginHarnessOwnsTransport &&
        !lockedProfileId &&
        isForwardablePluginHarnessAuthProfile(preferredProfileId)  // 允许转发
          ? preferredProfileId   // 满足 → 转发首选认证
          : undefined;          // 不满足 → 不转发
      // 普通模式 + 有锁定认证 → 校验认证是否【合法、能用】
      if (lockedProfileId && !pluginHarnessOwnsTransport) {
        // 检查这个认证是否适用于当前服务商
        const eligibility = resolveAuthProfileEligibility({
          cfg: params.config,
          store: authStore,
          provider,
          profileId: lockedProfileId,
        });
        // 不合法 → 直接抛错，终止运行
        if (!eligibility.eligible) {
          throw new Error(`Auth profile "${lockedProfileId}" is not configured for ${provider}.`);
        }
      }
      // 生成【认证尝试顺序列表】（主密钥→备用密钥）
      const profileOrder = shouldPreferExplicitConfigApiKeyAuth(params.config, provider)
        ? []  // 如果用环境变量/配置文件密钥 → 不用列表
        : resolveAuthProfileOrder({
            cfg: params.config,
            store: authStore,
            provider,
            preferredProfile: preferredProfileId,  // 首选认证排第一
          });
      // 服务商（如OpenAI）自己推荐的认证ID
      const providerPreferredProfileId = lockedProfileId
        ? undefined  // 有用户锁定 → 不使用服务商推荐
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
      // 构建认证候选顺序
      const providerOrderedProfiles =
        providerPreferredProfileId && profileOrder.includes(providerPreferredProfileId)
          ? [
              providerPreferredProfileId,
              ...profileOrder.filter((profileId) => profileId !== providerPreferredProfileId),
            ]
          : profileOrder;
      // 把服务商推荐的密钥放到最前面优先尝试。
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
      // 插件模式下，筛选出【允许转发给插件】的认证列表
      const pluginHarnessForwardedProfileCandidates = pluginHarnessOwnsTransport
        ? profileCandidates.filter(isForwardablePluginHarnessAuthProfile)
        : [];
      // 决定用哪个存储来记录认证失败（插件/普通模式分开）
      const profileFailureStore = pluginHarnessOwnsTransport ? attemptAuthProfileStore : authStore;
      // 当前正在尝试第几个认证（从第0个开始）
      let profileIndex = 0;
      // 日志：记录所有认证尝试过程（方便排查问题）
      const traceAttempts: TraceAttempt[] = [];

      // 初始思考模式：默认关闭（off）
      const initialThinkLevel = params.thinkLevel ?? "off";
      // 当前实际使用的思考模式（可能会动态变化）
      let thinkLevel = initialThinkLevel;
      // 记录已经尝试过哪些思考模式（避免重复试）
      const attemptedThinking = new Set<ThinkLevel>();
      // 当前使用的 API Key 信息（密钥、权限、所属用户）
      let apiKeyInfo: ApiKeyInfo | null = null;
      // 上一次使用的认证 profile ID
      let lastProfileId: string | undefined;
      // 运行时认证状态（是否登录、是否过期、是否需要刷新）
      let runtimeAuthState: RuntimeAuthState | null = null;
      // 标记：是否取消了认证刷新流程
      let runtimeAuthRefreshCancelled = false;
      const {
        advanceAuthProfile,   // 自动换下一个密钥（密钥坏了/限流了）
        initializeAuthProfile,   // 初始化当前选中的密钥
        maybeRefreshRuntimeAuthForAuthError,  // 密钥过期自动刷新
        stopRuntimeAuthRefreshTimer,    // 停止刷新定时器
      } = createEmbeddedRunAuthController({  // 认证控制器，所有和密钥、权限、重试相关的逻辑全部交给它管
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
      // 插件模式专用的 “自动换密钥” 函数
      // 当前密钥坏了 / 限流了 → 自动找到下一个可用、安全、没被冷却的密钥，切换过去。
      const advancePluginHarnessAuthProfile = async (): Promise<boolean> => {
        // 1. 不是插件模式 或 密钥被用户锁定 → 不能自动切换，直接 return false
        if (!pluginHarnessOwnsTransport || lockedProfileId) {
          return false;
        }
        // 2. 从【下一个索引】开始找可用密钥
        let nextIndex = profileIndex + 1;
        // 3. 遍历所有候选密钥，找一个能用的
        while (nextIndex < profileCandidates.length) {
          const candidate = profileCandidates[nextIndex];
          // 情况A：没有密钥 或 密钥不允许转发给插件 → 跳过
          if (!candidate || !isForwardablePluginHarnessAuthProfile(candidate)) {
            nextIndex += 1;
            continue;
          }
          // 情况B：密钥正在冷却（限流/刚报错）→ 暂时不能用，跳过
          if (isProfileInCooldown(attemptAuthProfileStore, candidate, undefined, modelId)) {
            nextIndex += 1;
            continue;
          }
          // 找到可用密钥！切换过去
          profileIndex = nextIndex;    // 更新当前索引
          lastProfileId = candidate;   // 记录当前用的密钥
          thinkLevel = initialThinkLevel;   // 重置思考模式
          attemptedThinking.clear();   // 清空尝试记录
          return true;       // 切换成功
        }
        // 所有密钥都试完了，没可用的 → 切换失败
        return false;
      };

      // Plugin harnesses own their model transport/auth. Running PI's generic
      // 关键注释：插件自己管认证，不能走系统通用认证流程，否则会冲突
      // auth bootstrap here can turn synthetic provider markers into real
      // vendor-token refresh attempts before the plugin gets control.
      if (!pluginHarnessOwnsTransport) {
        // 普通模式：执行系统标准的认证初始化
        await initializeAuthProfile();
      } else if (lockedProfileId) {
        // 插件模式 + 用户锁定认证：直接记录使用的认证ID
        lastProfileId = lockedProfileId;
      } else if (forwardedPluginHarnessProfileId) {
        // 插件模式 + 转发认证：记录要转发给插件的认证
        lastProfileId = forwardedPluginHarnessProfileId;
      }
      // 标记：认证阶段全部完成
      startupStages.mark("auth");
      // 通知系统：进入认证完成后的阶段
      notifyExecutionPhase("auth", { provider, model: modelId });
      // 创建本次运行使用的【作用域认证存储】
      // 插件模式下只暴露允许转发的认证，保证安全
      const runAttemptAuthProfileStore = pluginHarnessOwnsTransport
        ? createScopedAuthProfileStore(
            attemptAuthProfileStore,
            pluginHarnessForwardedProfileCandidates.length > 0
              ? pluginHarnessForwardedProfileCandidates
              : lastProfileId,
          )
        : attemptAuthProfileStore;
      // 解析会话相关ID
      const { sessionAgentId } = resolveSessionAgentIds({
        sessionKey: params.sessionKey,
        config: params.config,
        agentId: params.agentId,
      });
      // 解析当前会话应该使用的【执行契约】（运行规则）
      const configuredExecutionContract =
        resolveAgentExecutionContract(params.config, sessionAgentId) ?? "default";
      // 判断：是否启用【严格智能体模式】（严格的工具调用、多步推理）
      const strictAgenticActive = isStrictAgenticExecutionContractActive({
        config: params.config,
        sessionKey: params.sessionKey,
        agentId: params.agentId,
        provider,
        modelId,
      });
      // 最终确定执行契约（行为模式）
      const executionContract = strictAgenticActive ? "strict-agentic" : "default";
      // 纯规划步骤最多重试几次
      const maxPlanningOnlyRetryAttempts = resolvePlanningOnlyRetryLimit(executionContract);
      // 纯推理步骤最多重试几次（固定默认值）
      const maxReasoningOnlyRetryAttempts = DEFAULT_REASONING_ONLY_RETRY_LIMIT;
      // 模型返回空结果时最多重试几次
      const maxEmptyResponseRetryAttempts = DEFAULT_EMPTY_RESPONSE_RETRY_LIMIT;

      // 超时压缩重试次数（超时了就缩短上下文重试，最多试2次）
      const MAX_TIMEOUT_COMPACTION_ATTEMPTS = 2;
      // 内容溢出（token太长）重试次数（最多试3次）
      const MAX_OVERFLOW_COMPACTION_ATTEMPTS = 3;
      // 整个运行循环最大迭代次数（根据密钥数量动态计算） 
      const MAX_RUN_LOOP_ITERATIONS = resolveMaxRunRetryIterations(profileCandidates.length);
      // token 溢出（内容太长）重试次数
      let overflowCompactionAttempts = 0;
      // 是否做过工具结果截断
      let toolResultTruncationAttempted = false;
      // 记录提示词警告签名（内部调试用）
      let bootstrapPromptWarningSignaturesSeen =
        params.bootstrapPromptWarningSignaturesSeen ??
        (params.bootstrapPromptWarningSignature ? [params.bootstrapPromptWarningSignature] : []);
      // 统计 token 用量（总消耗）
      const usageAccumulator = createUsageAccumulator();
      // 上一次对话的 token 使用量
      let lastRunPromptUsage: ReturnType<typeof normalizeUsage> | undefined;
      // 自动压缩上下文次数
      let autoCompactionCount = 0;
      // 上一次压缩后的 token 数量
      let lastCompactionTokensAfter: number | undefined;
      // 主循环跑了多少次
      let runLoopIterations = 0;
      // 因为过载（限流）切换了多少次密钥
      let overloadProfileRotations = 0;
      // 规划步骤重试次数
      let planningOnlyRetryAttempts = 0;
      // 推理步骤重试次数
      let reasoningOnlyRetryAttempts = 0;
      // 空返回重试次数
      let emptyResponseRetryAttempts = 0;
      // 压缩后重试次数
      let compactionContinuationRetryAttempts = 0;
      // 超时重试次数
      let sameModelIdleTimeoutRetries = 0;
      // Cost-runaway breaker for #76293. State lives at the run-loop level
      // on purpose so it survives across attempt boundaries and across
      // profile/auth retries within this embedded run (a wrapper-local
      // counter would reset on every iteration). The helper is pure and
      // unit-tested in run/idle-timeout-breaker.test.ts; the run loop just
      // feeds it the outcome of each attempt.
      // 创建【空闲超时熔断保护器】
      // 作用：防止AI一直卡住、长时间不响应、无限空转（防卡死、防烧钱）
      const idleTimeoutBreakerState = createIdleTimeoutBreakerState();
      // Post-compaction loop guard for #77474. Armed at each compaction-success
      // site below; observed from the live tool-outcome path so it can abort
      // while the post-compaction prompt is still running.
      // 读取配置：工具循环检测规则（防止AI重复调用同一个工具死循环）
      const resolvedLoopDetectionConfig = resolveToolLoopDetectionConfig({
        cfg: params.config,
        agentId: sessionAgentId,
      });
      // 创建【压缩后循环保护器】
      // 作用：AI压缩上下文后，防止进入死循环重复执行
      const postCompactionGuard = createPostCompactionLoopGuard(
        resolvedLoopDetectionConfig?.postCompactionGuard,
        { enabled: resolvedLoopDetectionConfig?.enabled !== false },
      );
      // 中断控制器：发现死循环 → 立刻强制终止AI运行
      let postCompactionAbortController: AbortController | undefined;
      // 记录中断错误信息
      let postCompactionAbortError: PostCompactionLoopPersistedError | undefined;
      // 监听工具执行结果：一旦发现死循环 → 立刻触发中断，停止运行
      const observePostCompactionToolOutcome = (
        observation: PostCompactionGuardObservation,
      ): void => {
        const verdict = postCompactionGuard.observe(observation);
        if (verdict.shouldAbort) {
          // 终止！
          postCompactionAbortError ??= PostCompactionLoopPersistedError.fromVerdict(verdict);
          postCompactionAbortController?.abort(postCompactionAbortError);
        }
      };
      // 最后一次重试降级的原因（密钥错？限流？模型挂了？）
      let lastRetryFailoverReason: FailoverReason | null = null;
      
      // 各种重试时要发给AI的提示语
      let planningOnlyRetryInstruction: string | null = null;
      let reasoningOnlyRetryInstruction: string | null = null;
      let emptyResponseRetryInstruction: string | null = null;
      let compactionContinuationRetryInstruction: string | null = null;
      
      // 强制覆盖下一次提示词（临时改prompt用）
      let nextAttemptPromptOverride: string | null = null;
      // 执行快速通道指令（优化速度用）
      const ackExecutionFastPathInstruction = resolveAckExecutionFastPathInstruction({
        provider,
        modelId,
        prompt: params.prompt,
      });
      // 因为限流切换了几次密钥
      let rateLimitProfileRotations = 0;
      // 超时后压缩上下文重试次数
      let timeoutCompactionAttempts = 0;
      // Silent-error retry: non-strict-agentic models (e.g. ollama/glm-5.1) can
      // end a turn with stopReason="error" + zero output tokens, producing no
      // user-visible text. This is an orthogonal, model-agnostic resubmission
      // for errored turns; stopReason="stop" empty zero-token turns use the
      // visible-answer retry instruction instead.
      // 静默错误重试：模型悄悄报错但不告诉用户，最多重试3次
      const MAX_EMPTY_ERROR_RETRIES = 3;
      let emptyErrorRetries = 0;

      // 过载/限流后的重试等待时间、切换密钥上限
      const overloadFailoverBackoffMs = resolveOverloadFailoverBackoffMs(params.config);
      const overloadProfileRotationLimit = resolveOverloadProfileRotationLimit(params.config);
      const rateLimitProfileRotationLimit = resolveRateLimitProfileRotationLimit(params.config);
      
      // 当前活跃会话ID、会话文件
      let activeSessionId = params.sessionId;
      let activeSessionFile = params.sessionFile;
      // 会话消息持久化控制（是否存历史记录）
      let suppressNextUserMessagePersistence = params.suppressNextUserMessagePersistence ?? false;
      // Pi owns JSONL persistence; this marker only lets the outer retry avoid
      // replaying the same inbound channel message after overflow compaction.
      let lastPersistedCurrentMessageId: string | number | undefined;
      // 消息保存回调：用户消息存盘后记录一下ID，避免重复存储
      const onUserMessagePersisted: RunEmbeddedPiAgentParams["onUserMessagePersisted"] = (
        message,
      ) => {
        if (params.currentMessageId !== undefined) {
          lastPersistedCurrentMessageId = params.currentMessageId;
        }
        params.onUserMessagePersisted?.(message);
      };
      // 让AI从**当前对话中断处继续执行**，而不是重新开始
      const continueFromCurrentTranscript = () => {
        // 强制下一次提示词使用“继续执行”指令
        nextAttemptPromptOverride = MID_TURN_PRECHECK_CONTINUATION_PROMPT;
        // 抑制：不把这次继续操作当成新的用户消息存储
        suppressNextUserMessagePersistence = true;
      };
      // 限流熔断：如果轮换密钥太多次还限流 → 升级为“模型降级”
      const maybeEscalateRateLimitProfileFallback = (params: {
        failoverProvider: string;
        failoverModel: string;
        logFallbackDecision: (decision: "fallback_model", extra?: { status?: number }) => void;
      }) => {
        // 1. 限流计数器 +1（又遇到一次限流）
        rateLimitProfileRotations += 1;
        
        // 2. 没达到上限 / 没有配置降级 → 不处理，继续重试
        if (rateLimitProfileRotations <= rateLimitProfileRotationLimit || !fallbackConfigured) {
          return;
        }
        // 3. 达到上限！不能再轮换密钥了，必须降级
        const status = resolveFailoverStatus("rate_limit");
        log.warn(
          `rate-limit profile rotation cap reached for ${sanitizeForLog(provider)}/${sanitizeForLog(modelId)} after ${rateLimitProfileRotations} rotations; escalating to model fallback`,
        );
        // 记录降级日志
        params.logFallbackDecision("fallback_model", { status });
        // 4. 抛出降级错误 → 系统会自动换模型/服务商
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
      // 标记某个认证（API Key）调用失败
      const maybeMarkAuthProfileFailure = async (failure: {
        profileId?: string;     // 哪个密钥
        reason?: AuthProfileFailureReason | null;    // 失败原因
        config?: RunEmbeddedPiAgentParams["config"];
        agentDir?: RunEmbeddedPiAgentParams["agentDir"];
        modelId?: string;
      }) => {
        const { profileId, reason } = failure;
        // 1. 如果没有密钥 / 没有原因 / 是超时错误 → 不标记
        if (!profileId || !reason || reason === "timeout") {
          return;
        }
        // 2. 正式记录：这个密钥 × 失败了 × 原因是啥
        await markAuthProfileFailure({
          store: profileFailureStore,    // 存在哪里
          profileId,    // 密钥ID
          reason,          // 失败原因（invalid、rate_limited、forbidden 等）
          cfg: params.config,
          agentDir,
          runId: params.runId,
          modelId: failure.modelId,
        });
      };
      // 把「降级原因」转换成「认证失败原因」
      const resolveRunAuthProfileFailureReason = (failoverReason: FailoverReason | null) =>
        resolveAuthProfileFailureReason({
          failoverReason,  // 为什么降级？（限流？超时？密钥无效？）
          policy: params.authProfileFailurePolicy,   // 标记失败的策略
        });
      //当 AI 服务商返回【过载 / 繁忙】时，先等一会儿（退避），再重试 / 换密钥，避免疯狂轰炸服务器。
      const maybeBackoffBeforeOverloadFailover = async (reason: FailoverReason | null) => {
        // 1. 不是过载错误 或 不需要等待 → 直接跳过
        if (reason !== "overloaded" || overloadFailoverBackoffMs <= 0) {
          return;
        }
        // 2. 打印日志：服务过载，等待 X 毫秒
        log.warn(
          `overload backoff before failover for ${provider}/${modelId}: delayMs=${overloadFailoverBackoffMs}`,
        );
        try {
          // 3. 等待指定时间（支持中途取消）
          await sleepWithAbort(overloadFailoverBackoffMs, params.abortSignal);
        } catch (err) {
          // 4. 如果中途被取消 → 抛出终止错误
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
      // 提前初始化并复用 “长记忆 / 上下文工具”，避免每次重试都重新加载，浪费时间。
      // 全局只初始化一次上下文引擎，所有重试都复用它
      // 避免每次失败重试都重新创建、重连，大幅提升速度
      ensureContextEnginesInitialized();
      // 加载/获取上下文引擎（负责记忆、文件、工具、长上下文）
      const contextEngine = await resolveContextEngine(params.config, {
        agentDir,
        workspaceDir: resolvedWorkspace,
      });
      // 上下文引擎归属的插件ID（内部用）
      const contextEnginePluginId = resolveContextEngineOwnerPluginId(contextEngine);
      // 标记阶段：上下文引擎就绪
      startupStages.mark("context-engine");
      notifyExecutionPhase("context_engine", { provider, model: modelId });
      try {
        // 生成当前活跃的钩子上下文（给插件/扩展用）
        const resolveActiveHookContext = () => ({
          ...hookCtx,
          sessionId: activeSessionId,
        });
        // 接收压缩后的新会话信息（更新会话ID/文件）
        const adoptCompactionTranscript = (
          compactResult: Awaited<ReturnType<typeof contextEngine.compact>>,
        ) => {
          // 如果压缩后生成了新会话 → 更新当前会话
          const nextSessionId = compactResult.result?.sessionId;
          const nextSessionFile = compactResult.result?.sessionFile;
          if (nextSessionId && nextSessionId !== activeSessionId) {
            activeSessionId = nextSessionId;
          }
          if (nextSessionFile && nextSessionFile !== activeSessionFile) {
            activeSessionFile = nextSessionFile;
          }
        };
        // 上下文压缩事件通知（告诉外部：开始压缩 / 压缩完成）
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
        // 执行【压缩前】的生命周期钩子（给插件用）
        const runOwnsCompactionBeforeHook = async (reason: string) => {
          if (
            contextEngine.info.ownsCompaction !== true ||
            !hookRunner?.hasHooks("before_compaction")
          ) {
            return;
          }
          try {
            // 触发插件：before_compaction
            await hookRunner.runBeforeCompaction(
              { messageCount: -1, sessionFile: activeSessionFile },
              resolveActiveHookContext(),
            );
          } catch (hookErr) {
            log.warn(`before_compaction hook failed during ${reason}: ${String(hookErr)}`);
          }
        };
        // 上下文压缩【完成后】执行的钩子
        const runOwnsCompactionAfterHook = async (
          reason: string,   // 为什么压缩（比如token溢出）
          compactResult: Awaited<ReturnType<typeof contextEngine.compact>>,   // 压缩结果
        ) => {
          // 如果不满足条件 → 直接跳过
          if (
            contextEngine.info.ownsCompaction !== true ||    // 不是引擎负责压缩
            !compactResult.ok ||                           // 压缩失败
            !compactResult.compacted ||                    // 根本没压缩
            !hookRunner?.hasHooks("after_compaction")       // 没有插件监听
          ) {
            return;
          }
          try {
            // 触发所有插件的 after_compaction 事件
            await hookRunner.runAfterCompaction(
              {
                messageCount: -1,
                compactedCount: -1,
                tokenCount: compactResult.result?.tokensAfter,   // 压缩后的token数量
                sessionFile: compactResult.result?.sessionFile ?? activeSessionFile,
              },
              resolveActiveHookContext(),    // 带上当前会话信息
            );
          } catch (hookErr) {
            // 插件报错不影响主流程
            log.warn(`after_compaction hook failed during ${reason}: ${String(hookErr)}`);
          }
        };
        // 是否有认证重试正在进行中（防止重复触发）
        let authRetryPending = false;
        // 累积的会话回放状态：出错重试时，能恢复到之前的对话现场
        let accumulatedReplayState = createEmbeddedRunReplayState();
        // Hoisted so the retry-limit error path can use the most recent API total.
        // 最后一轮对话的 token 总数（提升到外层，方便报错时使用） 
        let lastTurnTotal: number | undefined;
        // 无限循环：AI 重试、换模型、换密钥、压缩上下文都在这里跑
        while (true) {
          // 如果重试次数达到上限 → 终止，不再尝试
          if (runLoopIterations >= MAX_RUN_LOOP_ITERATIONS) {
            const message =
              `Exceeded retry limit after ${runLoopIterations} attempts ` +
              `(max=${MAX_RUN_LOOP_ITERATIONS}).`;
            // 打印错误日志
            log.error(
              `[run-retry-limit] sessionKey=${params.sessionKey ?? params.sessionId} ` +
                `provider=${provider}/${modelId} attempts=${runLoopIterations} ` +
                `maxAttempts=${MAX_RUN_LOOP_ITERATIONS}`,
            );
            // 决定最终怎么处理这个失败（降级？抛错？）
            const retryLimitDecision = resolveRunFailoverDecision({
              stage: "retry_limit",
              fallbackConfigured,
              failoverReason: lastRetryFailoverReason,
            });
            // 执行最终失败处理：返回错误、上报监控、清理状态
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
          // 主循环次数 +1（本轮正式开始）
          runLoopIterations += 1;
          // 记录：上一轮是不是认证重试
          const runtimeAuthRetry = authRetryPending;
          // 重置重试标记（本轮开始，清空pending状态）
          authRetryPending = false;
          // 记录：本轮使用了哪种思考模式（避免重复尝试无效模式）
          attemptedThinking.add(thinkLevel);
          // 确保工作目录存在（存放临时文件、会话、缓存）
          await fs.mkdir(resolvedWorkspace, { recursive: true });

          // 构建基础提示词
          // 优先级：临时覆盖 > 原始提示词（如果是Anthropic，清理拒绝词）
          const basePrompt =
            nextAttemptPromptOverride ??   // 重试/继续指令优先
            (provider === "anthropic" ? scrubAnthropicRefusalMagic(params.prompt) : params.prompt);
          // 用完立刻清空，防止影响下一轮
          nextAttemptPromptOverride = null;
          // 收集所有额外指令（快速通道、各种重试提示）
          const promptAdditions = [
            ackExecutionFastPathInstruction,      // 快速通道指令
            planningOnlyRetryInstruction,          // 规划重试提示
            reasoningOnlyRetryInstruction,         // 推理重试提示
            emptyResponseRetryInstruction,         // 空返回重试提示
            compactionContinuationRetryInstruction,   // 压缩后继续提示
          ].filter(
            (value): value is string => typeof value === "string" && value.trim().length > 0,   // 过滤掉空的、无效的
          );
          // 拼接最终提示词：基础内容 + 额外指令
          const prompt =
            promptAdditions.length > 0
              ? `${basePrompt}\n\n${promptAdditions.join("\n\n")}`
              : basePrompt;
          // 准备API密钥
          let resolvedStreamApiKey: string | undefined;
          if (!runtimeAuthState && apiKeyInfo) {
            resolvedStreamApiKey = (apiKeyInfo as ApiKeyInfo).apiKey;
          }
          // 构建AI运行时执行计划（任务单）
          const runtimePlan = buildAgentRuntimePlan({
            // 基础信息：服务商、模型、接口类型
            provider,
            modelId,
            model: effectiveModel,
            modelApi: effectiveModel.api,

            // 代理/插件执行环境
            harnessId: agentHarness.id,
            harnessRuntime: agentHarness.id,
            allowHarnessAuthProfileForwarding: pluginHarnessOwnsTransport,
            
            // 认证/密钥信息（当前用哪个API Key）
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

            // 系统环境
            config: params.config,
            workspaceDir: resolvedWorkspace,    // 工作目录
            agentDir,                      // 代理目录
            agentId: workspaceResolution.agentId,

            // 思考模式（深度思考 / 普通 / 快速）
            thinkingLevel: thinkLevel,
            
            // 流式参数、快速模式
            extraParamsOverride: {
              ...params.streamParams,
              fastMode: params.fastMode,
            },
          });
          // 只在第一次重试时发送一次启动完成信号
          if (!startupStagesEmitted) {
            // 标记阶段：开始发送请求
            startupStages.mark("attempt-dispatch");
            // 通知外部系统：进入「请求派发」阶段
            notifyExecutionPhase("attempt_dispatch", { provider, model: modelId });
            // 打印/上报启动阶段汇总日志
            emitStartupStageSummary("attempt-dispatch");
            // 标记为已发送，后续重试不再重复执行
            startupStagesEmitted = true;
          }
          // 为本次AI调用创建一个独立的中断控制器
          const attemptAbortController = new AbortController();
          // 让“压缩后循环保护”也共用这个中断器（发现死循环时可以终止）
          postCompactionAbortController = attemptAbortController;
          // 拿到外部传入的中断信号（比如用户手动取消）
          const parentAbortSignal = params.abortSignal;
          // 定义：外部中断 → 同步中断本次AI请求
          const relayParentAbort = (): void => {
            attemptAbortController.abort(parentAbortSignal?.reason);
          };
          // 如果外部已经取消 → 立刻中断
          if (parentAbortSignal?.aborted) {
            relayParentAbort();
          } else {
            // 否则监听外部中断事件，触发时同步取消
            parentAbortSignal?.addEventListener("abort", relayParentAbort, { once: true });
          }
          // 整个 AI 系统的 “开火按钮”: 整个 AI 系统真正执行、调用模型、生成回答、处理工具、流式输出的最终入口
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
            .catch((err: unknown): never => {  // 如果死循环保护器触发，优先抛这个错误, 否则抛原始错误, 保证死循环可以强制终止调用
              throw postCompactionAbortError ?? err;
            })
            .finally(() => {   // 清理工作：调用结束无论成功失败，都清理中断监听、释放控制器，防止内存泄漏
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
