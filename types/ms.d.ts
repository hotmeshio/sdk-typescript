declare module 'ms' {
  function ms(value: string): number;
  function ms(value: number): string;
  function ms(value: string | number): string | number;

  export = ms;
}
