import { Pipe } from "./pipe";

//TransitionMatch type: { expected: false, actual: '{a2.output.data.approved}' }
export type TransitionMatch = {
  expected: boolean | string | number | null;
  actual: boolean | string | number | null | { '@pipe': Pipe };
}

export type TransitionRule = {
  gate?: 'and'|'or'; //`and` is default
  code?: string; //200 is default; must be an eplicit 3-digit code and must have an associated output schema matching this value to compile
  match: Array<TransitionMatch>;
}

//this is format for how all transitions for a single app are returned from the datastore
export type Transitions = { 
  [key: string]: {
    [key: string]: TransitionRule
  }
}
