import { HMNS } from '../../modules/key';

export function verifyAndSetNamespace(
  instance: { namespace: string },
  namespace?: string,
) {
  if (!namespace) {
    instance.namespace = HMNS;
  } else if (!namespace.match(/^[A-Za-z0-9-]+$/)) {
    throw new Error(`config.namespace [${namespace}] is invalid`);
  } else {
    instance.namespace = namespace;
  }
}

export function verifyAndSetAppId(
  instance: { appId: string },
  appId: string,
) {
  if (!appId?.match(/^[A-Za-z0-9-]+$/)) {
    throw new Error(`config.appId [${appId}] is invalid`);
  } else if (appId === 'a') {
    throw new Error(`config.appId [${appId}] is reserved`);
  } else {
    instance.appId = appId;
  }
}
