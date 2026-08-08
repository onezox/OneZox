import 'server-only';

import path from 'node:path';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';

import { getSessionToken } from './session';
import { log, errorMessage } from './log';
import { NotAuthenticatedError } from './graphql';

/**
 * The MUTATION path — Phase-05 Step U2.
 *
 * Every panel mutation is a unary gRPC call to admin-api's AdminService
 * (proto/admin/v1/admin.proto), made SERVER-SIDE from a React Server
 * Action. This is the Step E decision made concrete: RSC runs in
 * Node.js, so a native gRPC client works directly — no grpc-web proxy,
 * no HTTP/JSON translation layer, and no path by which a browser ever
 * speaks to admin-api itself.
 *
 * The admin token is read from the httpOnly cookie here and attached as
 * gRPC metadata. It never crosses into the browser: `import
 * 'server-only'` makes a Client Component importing this file a build
 * error, not a runtime leak.
 *
 * Transport is insecure app-layer credentials, exactly like admin-api's
 * own dials to control-plane and provider-gateway — Cilium's
 * SPIFFE/SPIRE mTLS enforces transport security at the mesh layer
 * (admin-api-mtls's own admin-panel ingress rule, Step V).
 */

const ADMIN_API_ADDR =
  process.env.ADMIN_API_GRPC_ADDR ?? 'admin-api.default.svc.cluster.local:50051';

const PROTO_PATH = path.join(process.cwd(), 'proto/admin/v1/admin.proto');

interface AdminServiceClient extends grpc.Client {
  [method: string]: unknown;
}

let cachedClient: AdminServiceClient | null = null;

/**
 * Lazily built and then reused for the process lifetime — a gRPC
 * channel is designed to be long-lived and multiplexed; building one
 * per request would add a TCP+HTTP/2 handshake to every mutation and
 * defeat the mesh's own connection reuse.
 */
function client(): AdminServiceClient {
  if (cachedClient) return cachedClient;

  const definition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: false,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  const pkg = grpc.loadPackageDefinition(definition) as unknown as {
    admin: { v1: { AdminService: new (addr: string, creds: grpc.ChannelCredentials) => AdminServiceClient } };
  };

  cachedClient = new pkg.admin.v1.AdminService(
    ADMIN_API_ADDR,
    grpc.credentials.createInsecure(),
  );
  log.info('admin-api grpc client created', { addr: ADMIN_API_ADDR });
  return cachedClient;
}

/**
 * call invokes one unary RPC as the current session's admin identity.
 *
 * A PERMISSION_DENIED comes back as a plain Error whose message is
 * admin-api's own ("insufficient privileges for this action") — the
 * panel surfaces that verbatim. That is the RBAC guard (Step G) being
 * visible to the operator: the UI hides mutating controls from a
 * viewer, but the server is what actually refuses, and a viewer who
 * reaches the action anyway sees the real refusal rather than a
 * client-side illusion.
 */
async function call<Req extends object, Res>(method: string, req: Req): Promise<Res> {
  const token = await getSessionToken();
  if (!token) throw new NotAuthenticatedError();

  const metadata = new grpc.Metadata();
  metadata.set('authorization', `bearer ${token}`);

  const started = Date.now();
  return new Promise<Res>((resolve, reject) => {
    const fn = client()[method] as (
      req: Req,
      md: grpc.Metadata,
      cb: (err: grpc.ServiceError | null, res: Res) => void,
    ) => void;

    fn.call(client(), req, metadata, (err, res) => {
      if (err) {
        if (err.code === grpc.status.UNAUTHENTICATED) {
          log.warn('admin-api rejected the session', { method });
          reject(new NotAuthenticatedError());
          return;
        }
        log.error('admin-api mutation failed', {
          method,
          grpc_code: err.code,
          error: errorMessage(err),
        });
        reject(new Error(err.details || err.message));
        return;
      }
      log.info('admin-api mutation served', { method, latency_ms: Date.now() - started });
      resolve(res);
    });
  });
}

/*
 * One typed wrapper per mutating RPC. These six are admin.proto's
 * ENTIRE command surface — the same six the RBAC allow-list names
 * (authz.go) and Step R's audit sweep enumerated. There is no seventh,
 * and nothing here can construct one.
 */

export function publishModelVersion(req: { modelRef: string; specJson: string }) {
  return call<typeof req, { versionId: string }>('PublishModelVersion', req);
}

export function startRollout(req: {
  modelRef: string;
  versionId: string;
  strategyJson: string;
}) {
  return call<typeof req, { rolloutId: string }>('StartRollout', req);
}

export function promoteRollout(req: { rolloutId: string }) {
  return call<typeof req, { newStage: string }>('PromoteRollout', req);
}

export function abortRollout(req: { rolloutId: string }) {
  return call<typeof req, Record<string, never>>('AbortRollout', req);
}

export function createApiKey(req: { orgId: string; scopes: string[] }) {
  return call<typeof req, { keyId: string; rawKey: string }>('CreateApiKey', req);
}

export function revokeApiKey(req: { keyId: string }) {
  return call<typeof req, Record<string, never>>('RevokeApiKey', req);
}
