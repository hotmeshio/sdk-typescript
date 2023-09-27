type PipeItem = string | boolean | number | null;

type PipeItems = PipeItem[];

type Pipe = (PipeItem[] | Pipe)[];

export { Pipe, PipeItem, PipeItems };