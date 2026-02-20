import { CollationError, InactiveJobError } from '../../modules/errors';
import { CollationFaultType, CollationStage } from '../../types/collator';
import { ActivityDuplex } from '../../types/activity';
import { HotMeshGraph } from '../../types/hotmesh';
import { ProviderTransaction } from '../../types/provider';
import { Activity } from '../activities/activity';
import { Cycle } from '../activities/cycle';

class CollatorService {
  //max int digit count that supports `hincrby`
  static targetLength = 15;

  /**
   * Positional weights for the 15-digit activity/GUID ledger.
   *
   * Position:  1     2     3    4      5     6    7      8-15
   * Weight:   100T  10T   1T   100B   10B   1B   100M   10M..1
   */
  static WEIGHTS = {
    AUTH:           100_000_000_000_000,  // pos 1: reserved (trigger seed only)
    FINALIZE:       200_000_000_000_000,  // pos 1: +200T sets pos 1 to 2 (finalized)
    LEG1_ENTRY:       1_000_000_000_000,  // pos 3: Leg1 attempt counter (pos 2-3 = 0..99)
    LEG1_COMPLETE:      100_000_000_000,  // pos 4: Leg1 completion marker
    STEP1_WORK:          10_000_000_000,  // pos 5: Leg2 work done
    STEP2_SPAWN:          1_000_000_000,  // pos 6: children spawned
    STEP3_CLEANUP:          100_000_000,  // pos 7: job completion tasks done
    LEG2_ENTRY:                       1,  // pos 8-15: Leg2 entry counter
    GUID_SNAPSHOT:      100_000_000_000,  // 100B on GUID ledger (job closed snapshot)
  };

  /**
   * Upon re/entry, verify that the job status is active
   */
  static assertJobActive(
    status: number,
    jobId: string,
    activityId: string,
    threshold = 0,
  ): void {
    if (status <= threshold) {
      throw new InactiveJobError(jobId, status, activityId);
    }
  }

  /**
   * returns the dimensional address (dad) for the target; due
   * to the nature of the notary system, the dad for leg 2 entry
   * must target the `0` index while leg 2 exit must target the
   * current index (0)
   */
  static getDimensionalAddress(
    activity: Activity,
    isEntry = false,
  ): Record<string, string> {
    let dad = activity.context.metadata.dad || activity.metadata.dad;
    if (isEntry && dad && activity.leg === 2) {
      dad = `${dad.substring(0, dad.lastIndexOf(','))},0`;
    }
    return CollatorService.getDimensionsById(
      [...activity.config.ancestors, activity.metadata.aid],
      dad,
    );
  }

  /**
   * resolves the dimensional address for the
   * ancestor in the graph to go back to. this address
   * is determined by trimming the last digits from
   * the `dad` (including the target).
   * the target activity index is then set to `0`, so that
   * the origin node can be queried for approval/entry.
   */
  static resolveReentryDimension(activity: Cycle) {
    const targetActivityId = activity.config.ancestor;
    const ancestors = activity.config.ancestors;
    const ancestorIndex = ancestors.indexOf(targetActivityId);
    const dimensions = activity.metadata.dad.split(','); //e.g., `,0,0,1,0`
    dimensions.length = ancestorIndex + 1;
    dimensions.push('0');
    return dimensions.join(',');
  }

  // ──────────────────────────────────────────────────
  //  Leg1 notarization
  // ──────────────────────────────────────────────────

  /**
   * Leg1 entry: increment attempt counter (+1T).
   * NOT bundled with Leg1 work — exists only to mark entry.
   */
  static async notarizeEntry(
    activity: Activity,
    transaction?: ProviderTransaction,
  ): Promise<number> {
    const amount = await activity.store.collate(
      activity.context.metadata.jid,
      activity.metadata.aid,
      this.WEIGHTS.LEG1_ENTRY,
      this.getDimensionalAddress(activity),
      transaction,
    );
    this.verifyInteger(amount, 1, 'enter');
    return amount;
  }

  /**
   * Leg1 completion: increment +100B to mark Leg1 complete.
   * For cycle=true activities, also pre-seeds the Leg2 entry counter (+1)
   * so the first real Leg2 gets adjacentIndex=1 (new dimension).
   * MUST be bundled in the same transaction as Leg1 durable work.
   */
  static async notarizeLeg1Completion(
    activity: Activity,
    transaction?: ProviderTransaction,
  ): Promise<number> {
    const amount = activity.config.cycle
      ? this.WEIGHTS.LEG1_COMPLETE + this.WEIGHTS.LEG2_ENTRY
      : this.WEIGHTS.LEG1_COMPLETE;
    return await activity.store.collate(
      activity.context.metadata.jid,
      activity.metadata.aid,
      amount,
      this.getDimensionalAddress(activity),
      transaction,
    );
  }

  /**
   * Leg1 early exit: marks Leg1 complete for activities that
   * only run Leg1 and fully close (e.g., Cycle).
   * Increment +100B (Leg1 completion marker).
   */
  static async notarizeEarlyExit(
    activity: Activity,
    transaction?: ProviderTransaction,
  ): Promise<number> {
    return await activity.store.collate(
      activity.context.metadata.jid,
      activity.metadata.aid,
      this.WEIGHTS.LEG1_COMPLETE,
      this.getDimensionalAddress(activity),
      transaction,
    );
  }

  /**
   * Leg1 early completion: marks Leg1 complete for Leg1-only
   * activities that spawn children (e.g., Signal, Hook passthrough,
   * Interrupt-another). Increment +100B.
   */
  static async notarizeEarlyCompletion(
    activity: Activity,
    transaction?: ProviderTransaction,
  ): Promise<number> {
    return await activity.store.collate(
      activity.context.metadata.jid,
      activity.metadata.aid,
      this.WEIGHTS.LEG1_COMPLETE,
      this.getDimensionalAddress(activity),
      transaction,
    );
  }

  // ──────────────────────────────────────────────────
  //  Leg2 notarization (entry + 3-step protocol)
  // ──────────────────────────────────────────────────

  /**
   * Leg2 entry: atomically increments the activity ledger (+1) and
   * seeds the GUID ledger with the ordinal IF NOT EXISTS.
   * Returns [activityLedger, guidLedger] after the compound operation.
   */
  static async notarizeLeg2Entry(
    activity: Activity,
    guid: string,
    transaction?: ProviderTransaction,
  ): Promise<[number, number]> {
    const jid = activity.context.metadata.jid;
    const localMulti = transaction || activity.store.transact();
    //compound: increment activity Leg2 counter and seed GUID ledger
    await activity.store.collateLeg2Entry(
      jid,
      activity.metadata.aid,
      guid,
      this.getDimensionalAddress(activity, true),
      localMulti,
    );
    const results = await localMulti.exec();
    const result = results[0];
    const [amountConcrete, amountSynthetic] = Array.isArray(result)
      ? result
      : [result, result];
    this.verifyInteger(amountConcrete as number, 2, 'enter');
    this.verifySyntheticInteger(amountSynthetic as number);
    return [amountConcrete as number, amountSynthetic as number];
  }

  /**
   * Step 1: Mark Leg2 work done (+10B on GUID ledger only).
   * MUST be bundled with Leg2 durable work writes.
   */
  static async notarizeStep1(
    activity: Activity,
    guid: string,
    transaction: ProviderTransaction,
  ): Promise<void> {
    const jid = activity.context.metadata.jid;
    await activity.store.collateSynthetic(
      jid,
      guid,
      this.WEIGHTS.STEP1_WORK,
      transaction,
    );
  }

  /**
   * Step 2: Mark children spawned (+1B on GUID ledger only).
   * The job semaphore update and GUID job-closed snapshot are handled
   * by the compound `setStatusAndCollateGuid` primitive, which MUST
   * be called in the same transaction.
   */
  static async notarizeStep2(
    activity: Activity,
    guid: string,
    transaction: ProviderTransaction,
  ): Promise<void> {
    const jid = activity.context.metadata.jid;
    await activity.store.collateSynthetic(
      jid,
      guid,
      this.WEIGHTS.STEP2_SPAWN,
      transaction,
    );
  }

  /**
   * Step 3: Mark job completion tasks done (+100M on GUID ledger only).
   * MUST be bundled with job completion durable writes.
   */
  static async notarizeStep3(
    activity: Activity,
    guid: string,
    transaction: ProviderTransaction,
  ): Promise<void> {
    const jid = activity.context.metadata.jid;
    await activity.store.collateSynthetic(
      jid,
      guid,
      this.WEIGHTS.STEP3_CLEANUP,
      transaction,
    );
  }

  /**
   * Finalize: close the activity to new Leg2 GUIDs (+200T).
   * Sets pos 1 to 2 (finalized).
   * Only for non-cycle activities after final SUCCESS/ERROR.
   */
  static async notarizeFinalize(
    activity: Activity,
    transaction: ProviderTransaction,
  ): Promise<void> {
    if (!activity.config.cycle) {
      await activity.store.collate(
        activity.context.metadata.jid,
        activity.metadata.aid,
        this.WEIGHTS.FINALIZE,
        this.getDimensionalAddress(activity),
        transaction,
      );
    }
  }

  // ──────────────────────────────────────────────────
  //  GUID ledger extraction (step-level resume)
  // ──────────────────────────────────────────────────

  /**
   * Check if Step 1 (work done) is complete on the GUID ledger.
   * Position 5 (10B digit) > 0.
   */
  static isGuidStep1Done(guidLedger: number): boolean {
    return this.getDigitAtPosition(guidLedger, 5) > 0;
  }

  /**
   * Check if Step 2 (children spawned) is complete on the GUID ledger.
   * Position 6 (1B digit) > 0.
   */
  static isGuidStep2Done(guidLedger: number): boolean {
    return this.getDigitAtPosition(guidLedger, 6) > 0;
  }

  /**
   * Check if Step 3 (job completion tasks) is complete on the GUID ledger.
   * Position 7 (100M digit) > 0.
   */
  static isGuidStep3Done(guidLedger: number): boolean {
    return this.getDigitAtPosition(guidLedger, 7) > 0;
  }

  /**
   * Check if this GUID was responsible for closing the job.
   * Position 4 (100B digit) > 0 (job closed snapshot).
   */
  static isGuidJobClosed(guidLedger: number): boolean {
    return this.getDigitAtPosition(guidLedger, 4) > 0;
  }

  /**
   * Get the attempt count from the GUID ledger (last 8 digits).
   */
  static getGuidAttemptCount(guidLedger: number): number {
    return guidLedger % 100_000_000;
  }

  // ──────────────────────────────────────────────────
  //  Digit extraction
  // ──────────────────────────────────────────────────

  /**
   * Gets the digit at a 1-indexed position from a 15-digit ledger value.
   * The value is left-padded to 15 digits before extraction.
   */
  static getDigitAtPosition(num: number, position: number): number {
    const numStr = num.toString().padStart(this.targetLength, '0');
    if (position < 1 || position > this.targetLength) {
      return 0;
    }
    return parseInt(numStr[position - 1], 10);
  }

  /**
   * @deprecated Use getDigitAtPosition (1-indexed) instead
   */
  static getDigitAtIndex(num: number, targetDigitIndex: number): number | null {
    const numStr = num.toString();
    if (targetDigitIndex < 0 || targetDigitIndex >= numStr.length) {
      return null;
    }
    const digit = parseInt(numStr[targetDigitIndex], 10);
    return digit;
  }

  /**
   * Extracts the dimensional index from the Leg2 entry counter.
   * Non-cycle activities: first Leg2 → leg2Count=1 → 1-1=0 (same dimension as Leg1).
   * Cycle activities: first Leg2 → leg2Count=2 (pre-seeded +1) → 2-1=1 (new dimension).
   */
  static getDimensionalIndex(num: number): number | null {
    const leg2EntryCount = num % 100_000_000;
    if (leg2EntryCount <= 0) {
      return null;
    }
    return leg2EntryCount - 1;
  }

  // ──────────────────────────────────────────────────
  //  Verification
  // ──────────────────────────────────────────────────

  /**
   * Verifies the GUID ledger value for step-level resume decisions.
   * The GUID ledger is seeded with an ordinal position (last 8 digits)
   * on first entry; step markers drive all resume/reject logic.
   *
   * Fully processed: Step 3 done, or Steps 1+2 done without job closure.
   * Crash recovery: Any incomplete step combination is allowed for resume.
   */
  static verifySyntheticInteger(amount: number): void {
    const step2Done = this.isGuidStep2Done(amount);
    const step3Done = this.isGuidStep3Done(amount);
    const jobClosed = this.isGuidJobClosed(amount);

    if (step3Done) {
      //all steps complete; nothing more to do
      throw new CollationError(amount, 2, 'enter', CollationFaultType.INACTIVE);
    }
    if (step2Done && !jobClosed) {
      //steps 1+2 done but this GUID didn't close the job; no Step 3 needed
      throw new CollationError(amount, 2, 'enter', CollationFaultType.INACTIVE);
    }
    //all other cases: allow entry
    // - no steps done (fresh entry or pre-step-1 crash recovery)
    // - step 1 done, step 2 not (crash after step 1)
    // - steps 1+2 done, job closed, step 3 not (crash recovery for step 3)
  }

  /**
   * Verifies the activity ledger value at entry boundaries.
   *
   * Leg1 enter: pos 3 (1T digit) must be > 0 after +1T (proves seed exists).
   *             pos 4 (100B) must be 0 (Leg1 not yet complete).
   *             If pos 3 > 1 and pos 4 == 1, it's a stale/replayed message.
   *
   * Leg2 enter: pos 4 (100B) must be > 0 (Leg1 complete, reentry authorized).
   *             pos 1 (100T) must be < 2 (not finalized) — cycle activities exempt.
   */
  static verifyInteger(
    amount: number,
    leg: ActivityDuplex,
    stage: CollationStage,
  ): void {
    let faultType: CollationFaultType | undefined;
    if (leg === 1 && stage === 'enter') {
      const leg1Attempts = this.getDigitAtPosition(amount, 3);
      const leg1Complete = this.getDigitAtPosition(amount, 4);
      if (leg1Attempts === 0) {
        //seed was not set (no authorization)
        faultType = CollationFaultType.MISSING;
      } else if (leg1Complete > 0) {
        //Leg1 already completed — stale/replayed message
        faultType = CollationFaultType.DUPLICATE;
      }
    } else if (leg === 2 && stage === 'enter') {
      const leg1Complete = this.getDigitAtPosition(amount, 4);
      const finalized = this.getDigitAtPosition(amount, 1);
      if (leg1Complete === 0) {
        //Leg1 not complete — reentry not authorized
        faultType = CollationFaultType.FORBIDDEN;
      } else if (finalized >= 2) {
        //activity finalized (pos 1 = 2) — no new Leg2 GUIDs accepted
        faultType = CollationFaultType.INACTIVE;
      }
    }
    if (faultType) {
      throw new CollationError(amount, leg, stage, faultType);
    }
  }

  // ──────────────────────────────────────────────────
  //  Dimensional address resolution
  // ──────────────────────────────────────────────────

  static getDimensionsById(
    ancestors: string[],
    dad: string,
  ): Record<string, string> {
    //ancestors is an ordered list of all ancestors, starting with the trigger (['t1', 'a1', 'a2'])
    //dad is the dimensional address of the ancestors list (',0,5,3')
    //loop through the ancestors list and create a map of the ancestor to the dimensional address.
    //return { 't1': ',0', 'a1': ',0,5', 'a1': ',0,5,3', $ADJACENT: ',0,5,3,0' };
    // `adjacent` is a special key that is used to track the dimensional address of adjacent activities
    const map: Record<string, string> = { $ADJACENT: `${dad},0` };
    let dadStr = dad;
    ancestors.reverse().forEach((ancestor) => {
      map[ancestor] = dadStr;
      dadStr = dadStr.substring(0, dadStr.lastIndexOf(','));
    });
    return map;
  }

  // ──────────────────────────────────────────────────
  //  Seeds
  // ──────────────────────────────────────────────────

  /**
   * All trigger activities are assigned a status seed in a completed state.
   * Seed: 101100000000001 (authorized, 1 Leg1 entry, Leg1 complete, 1 Leg2 entry)
   */
  static getTriggerSeed(): string {
    return '101100000000001';
  }

  // ──────────────────────────────────────────────────
  //  Compiler
  // ──────────────────────────────────────────────────

  /**
   * entry point for compiler-type activities. This is called by the compiler
   * to bind the sorted activity IDs to the trigger activity. These are then used
   * at runtime by the activities to track job/activity status.
   * @param graphs
   */
  static compile(graphs: HotMeshGraph[]) {
    CollatorService.bindAncestorArray(graphs);
  }

  /**
   * binds the ancestor array to each activity.
   * Used in conjunction with the dimensional
   *  address (dad). If dad is `,0,1,0,0` and the
   * ancestor array is `['t1', 'a1', 'a2']` for
   * activity 'a3', then the SAVED DAD
   * will always have the trailing
   * 0's removed. This ensures that the addressing
   * remains consistent even if the graph changes.
   *   id    DAD           SAVED DAD
   * * t1 => ,0        =>  [empty]
   * * a1 => ,0,1      =>  ,0,1
   * * a2 => ,0,1,0    =>  ,0,1
   * * a3 => ,0,1,0,0  =>  ,0,1
   *
   */
  static bindAncestorArray(graphs: HotMeshGraph[]) {
    graphs.forEach((graph) => {
      const ancestors: Record<string, string[]> = {};
      const startingNode = Object.keys(graph.activities).find(
        (activity) => graph.activities[activity].type === 'trigger',
      );
      if (!startingNode) {
        throw new Error('collator-trigger-activity-not-found');
      }
      const dfs = (node: string, parentList: string[]) => {
        ancestors[node] = parentList;
        graph.activities[node]['ancestors'] = parentList;
        const transitions = graph.transitions?.[node] || [];
        transitions.forEach((transition) => {
          dfs(transition.to, [...parentList, node]);
        });
      };
      // Start the DFS traversal
      dfs(startingNode, []);
    });
  }

  /**
   * All activities exist on a dimensional plane. Zero
   * is the default. A value of
   * `AxY,0,0,0,0,1,0,0` would reflect that
   * an ancestor activity was dimensionalized beyond
   * the default.
   */
  static getDimensionalSeed(index = 0): string {
    return `,${index}`;
  }
}

export { CollatorService };
