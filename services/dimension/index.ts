import { HotMeshGraph } from "../../types/hotmesh";

class DimensionService {

  //max int digit count that supports `hincrby`
  static targetLength = 15; 

  /**
   * entry point for compiler-type activities. This is called by the compiler
   * to bind the sorted activity IDs to the trigger activity. These are then used
   * at runtime by the activities to track job/activity status.
   * @param graphs
   */
  static compile(graphs: HotMeshGraph[]) {

  }

  /**
   * All activities exist on a dimensional plane. Zero
   * is the default and is implied if no dimension is
   * present in the hash item key. EVERY value in the
   * job ledger is dimensionalized even if the dimension
   * is not present. The key, `AaA`, might not contain
   * a dimensional index, but it is still implicitly
   * dimensionalized as `AaA,0` (assuming a trigger).
   * A value of `AxY,0,0,0,0,1,0,0` would reflect that
   * an ancestor activity was dimensionalized beyond
   * the default. The dimensional string must
   * be included if not zero. There is likely a preceding
   * sibling dimension, so it would not need to include
   * the suffix, so these addresses are equivalent:
   * `AxY,0,0,0,0,0,0,0` == `AxY` for said sibling.
   */
  static getSeed(index = 0): string {
    return `,${index}`;
  }
}

export { DimensionService };
