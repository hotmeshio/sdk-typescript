// Shared in-process counter so the test can prove the framework actually
// retried the activity before the workflow caught the exhausted error.
export const state = {
  attempts: 0,
};
