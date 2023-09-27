interface App {
  name: string;
  title: string;
  description: string;
}

type AppVID = {
  version: string;
  id: string;
};

type AppTransitions = {
  [key: string]: Record<string, unknown>;
};

type AppSubscriptions = {
  [key: string]: string;
};

export { App, AppVID, AppTransitions, AppSubscriptions };
