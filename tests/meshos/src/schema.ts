import * as HotMeshTypes from '../../../types';

/**
 * Schema definition for 'widget-type' workflows. These properties are persisted
 * as indexed, searchable data fields on each workflow record.
 */
export const schema: HotMeshTypes.WorkflowSearchSchema = {
  /**
   * Entity tag for the workflow, representing its type. 
   * Indexed indicates if it's shared with the indexing service.
   */
  $entity: {
    type: 'TAG',
    indexed: false,
    primitive: 'string',
    required: true,
    examples: ['widget'],
  },

  /**
   * Widget ID, including the `widget-` entity prefix.
   */
  id: {
    type: 'TAG',
    primitive: 'string',
    required: true,
    examples: ['H56789'],
  },

  /**
   * Field indicating whether the widget is active ('y') or pruned ('n').
   */
  active: {
    type: 'TAG',
    primitive: 'string',
    required: true,
    examples: ['y', 'n'],
  },
};
