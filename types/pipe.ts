type PipeItem = string | boolean | number | null | object;

type PipeItems = PipeItem[];

type PipeObject = { '@pipe': Array<PipeItem[] | PipeObject | ReduceObject> };
type ReduceObject = { '@reduce': Array<PipeItem[] | PipeObject | ReduceObject> };

type Pipe = (PipeItem[] | PipeObject | ReduceObject)[];

type PipeContext = {
  $input: unknown[]; //input of the current iteration
  $output: unknown; //output of the current iteration and the final output for the reducer
  $item: unknown;  //target item in the iterator
  $key: string;    //array index as string or object key
  $index: number;  //numberic index of the iterator
};

export { Pipe, PipeContext, PipeItem, PipeItems, PipeObject, ReduceObject };