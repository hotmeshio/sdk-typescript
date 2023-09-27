 # Serializer Overview
 ## Sym Keys
 Each activity/job is granted 286 tranche of symbols (26 for metadata keys, 260 for data keys).
 
 Typically, 5-10 symbols are used for metadata (aid, atp, etc), but it allows for future expansion. The 260 remaining data symbols allow for 260 unique scalar mapping statements as it is the mapping statements (the paths to reach the target value) define the symbols to use for data mapping (the symbols represent these paths).
 
 If `a1` maps to `a1.output.data.abc` and `a2` maps to `a2.output.data.def`, then when `a1` saves its output, it will only save the field values `abc`, and `def`. When saved to the backend it would be: `{a1: {data: { abc: 'somevalue', def: 'another' }}` when flattened and deflated, the keys would be 'bbb' and 'bbc' (or whatever the actual value) and the values would be 'somevalue' and 'another' respectively.

## Sym Values
 It is possible to replace persisted values. Up to 2,704 possible 2-letter symbols are available for use (52^2) to represent any string value.
