import { CollationError } from '../../modules/errors';
import { RedisMulti } from '../../types/redis';
import { CollationFaultType, CollationStage } from '../../types/collator';
import { ActivityDuplex } from '../../types/activity';
import { HotMeshGraph } from '../../types/hotmesh';
import { Activity } from '../activities/activity';

class CollatorService {

  //max int digit count that supports `hincrby`
  static targetLength = 15;

  static getDimensionalAddress(activity: Activity): Record<string, string> {
    let dad = activity.context.metadata.dad || activity.metadata.dad;
    //todo: unsure about this reset
    // if (dad && activity.leg === 2) {
    //   console.log('setting dad index back to 0=>', dad);
    //   dad = `${dad.substring(0, dad.lastIndexOf(','))},0`;
    // }
    return CollatorService.getDimensionsById([...activity.config.ancestors, activity.metadata.aid], dad);
  }

  static async notarizeEntry(activity: Activity, multi?: RedisMulti): Promise<number> {
    //decrement by -100_000_000_000_000
    const amount = await activity.store.collate(activity.context.metadata.jid, activity.metadata.aid, -100_000_000_000_000, this.getDimensionalAddress(activity), multi);
    this.verifyInteger(amount, 1, 'enter');
    return amount;
  };

  static async authorizeReentry(activity: Activity, multi?: RedisMulti): Promise<number> {
    //set second digit to 8, allowing for re-entry
    //decrement by -10_000_000_000_000
    const amount = await activity.store.collate(activity.context.metadata.jid, activity.metadata.aid, -10_000_000_000_000, this.getDimensionalAddress(activity), multi);
    //this.verifyInteger(amount, 1, 'exit');
    return amount;
  }

  static async notarizeEarlyCompletion(activity: Activity, multi?: RedisMulti): Promise<number> {
    //initialize both `possible` (1m) and `actualized` (1) zero dimension, while decrementing the 2nd and 3rd digits to deactivate the activity
    return await activity.store.collate(activity.context.metadata.jid, activity.metadata.aid, 1_000_001 - 11_000_000_000_000, this.getDimensionalAddress(activity), multi);
  };

  static async notarizeReentry(activity: Activity, multi?: RedisMulti): Promise<number> {
    //increment by 1_000_000 (indicates re-entry and is used to drive the 'dimensional address' for adjacent activities (minus 1))
    const amount = await activity.store.collate(activity.context.metadata.jid, activity.metadata.aid, 1_000_000, this.getDimensionalAddress(activity), multi);
    this.verifyInteger(amount, 2, 'enter');
    return amount;
  };

  static async notarizeContinuation(activity: Activity, multi?: RedisMulti): Promise<number> {
    //keep open; actualize the leg2 dimension (+1)
    return await activity.store.collate(activity.context.metadata.jid, activity.metadata.aid, 1, this.getDimensionalAddress(activity), multi);
  };

  static async notarizeCompletion(activity: Activity, multi?: RedisMulti): Promise<number> {
    //close out; actualize leg2 dimension (+1) and decrement the 3rd digit (-1_000_000_000_000)
    return await activity.store.collate(activity.context.metadata.jid, activity.metadata.aid, 1 - 1_000_000_000_000, this.getDimensionalAddress(activity), multi);
  };

  static getDigitAtIndex(num: number, targetDigitIndex: number): number | null {
    const numStr = num.toString();
    if (targetDigitIndex < 0 || targetDigitIndex >= numStr.length) {
      return null;
    }
    const digit = parseInt(numStr[targetDigitIndex], 10);
    return digit;
  }

  static getDimensionalIndex(num: number): number | null {
    const numStr = num.toString();
    if (numStr.length < 9) {
      return null;
    }
    const extractedStr = numStr.substring(3, 9);
    const extractedInt = parseInt(extractedStr, 10);
    return extractedInt - 1;
  }

  static isDuplicate(num: number, targetDigitIndex: number): boolean {
    return this.getDigitAtIndex(num, targetDigitIndex) < 8;
  }

  static isInactive(num: number): boolean {
    return this.getDigitAtIndex(num, 2) < 9;
  }

  static isPrimed(amount: number, leg: ActivityDuplex): boolean {
    //activity entry is not allowed if paths not properly pre-set
    if (leg == 1) {
      return amount != -100_000_000_000_000;
    } else {
      return this.getDigitAtIndex(amount, 0) < 9 &&
        this.getDigitAtIndex(amount, 1) < 9;
    }
  }

  static verifyInteger(amount: number, leg: ActivityDuplex, stage: CollationStage): void {
    let faultType: CollationFaultType | undefined;
    if (leg === 1 && stage === 'enter') {
      if (!this.isPrimed(amount, 1)) {
        faultType = CollationFaultType.MISSING;
      } else if (this.isDuplicate(amount, 0)) {
        faultType = CollationFaultType.DUPLICATE;
      } else if (amount != 899_000_000_000_000) {
        faultType = CollationFaultType.INVALID;
      }
    } else if (leg === 1 && stage === 'exit') {
      if (amount === -10_000_000_000_000) {
        faultType = CollationFaultType.MISSING;
      } else if (this.isDuplicate(amount, 1)) {
        faultType = CollationFaultType.DUPLICATE;
      }
    } else if (leg === 2 && stage === 'enter') {
      if (!this.isPrimed(amount, 2)) {
        faultType = CollationFaultType.FORBIDDEN;
      } else if (this.isInactive(amount)) {
        faultType = CollationFaultType.INACTIVE;
      }
    }
    if (faultType) {
      throw new CollationError(amount, leg, stage, faultType);
    }
  }

  static getDimensionsById(ancestors: string[], dad: string): Record<string, string> {
    //ancestors is an ordered list of all ancestors, starting with the trigger (['t1', 'a1', 'a2'])
    //dad is the dimensional address of the ancestors list (',0,5,3')
    //loop through the ancestors list and create a map of the ancestor to the dimensional address.
    //return { 't1': ',0', 'a1': ',0,5', 'a1': ',0,5,3', $ADJACENT: ',0,5,3,0' };
    // `adjacent` is a special key that is used to track the dimensional address of adjacent activities
    const map: Record<string, string> = { '$ADJACENT': `${dad},0` };
    let dadStr = dad;
    ancestors.reverse().forEach((ancestor) => {
      map[ancestor] = dadStr;
      dadStr = dadStr.substring(0, dadStr.lastIndexOf(','));
    });
    return map;
  }

  /**
   * All non-trigger activities are assigned a status seed by their parent
   */
  static getSeed(): string {
    return '999000000000000';
  }

  /**
   * All trigger activities are assigned a status seed in a completed state
   */
  static getTriggerSeed(): string {
    return '888000001000001';
  }

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
        (activity) => graph.activities[activity].type === 'trigger'
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

  static isActivityComplete(status: number): boolean {
    return (status - 0) <= 0;
  }
}

export { CollatorService };
