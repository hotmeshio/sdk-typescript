const state = {
  count: 0,
};

async function example(count: number = 2): Promise<number> {
  if (state.count++ < count) {
    throw new Error('recurring-test-error');
  }
  return count;
}

export { example, state };
