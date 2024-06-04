export type CollationStage = 'enter' | 'exit' | 'confirm';

export enum CollationFaultType {
  MISSING = 'missing', //`as` uninitialized; leg1 entry not allowed
  DUPLICATE = 'duplicate', //1st digit < 8
  INACTIVE = 'inactive', //3rd digit is 8
  INVALID = 'invalid', //unknown value (corrupt for unknown reasons)
  FORBIDDEN = 'forbidden', //leg 1 not completed; reentry (leg 2) not allowed
}
