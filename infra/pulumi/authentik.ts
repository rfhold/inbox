import * as authentik from "@pulumi/authentik";
import * as pulumi from "@pulumi/pulumi";
import * as random from "@pulumi/random";

export interface BrowserApplicationArgs {
  displayName: string;
  slug: string;
  issuerBaseUrl: string;
  publicUrl: string;
  signingKeyId: pulumi.Input<string>;
  protectData: boolean;
}

export class BrowserApplication extends pulumi.ComponentResource {
  readonly clientId: pulumi.Output<string>;
  readonly clientSecret: pulumi.Output<string>;
  readonly issuer: pulumi.Output<string>;

  constructor(name: string, args: BrowserApplicationArgs, opts?: pulumi.ComponentResourceOptions) {
    super("inbox:authentik:BrowserApplication", name, args, opts);
    const child = { parent: this, protect: args.protectData };
    const authorizationFlow = authentik.getFlowOutput({ slug: "default-provider-authorization-implicit-consent" }, { parent: this });
    const invalidationFlow = authentik.getFlowOutput({ slug: "default-provider-invalidation-flow" }, { parent: this });
    const secret = new random.RandomPassword(`${name}-client-secret`, { length: 64, special: false }, child);
    const mappings = ["openid", "profile", "email"].map((scope) =>
      authentik.getPropertyMappingProviderScopeOutput({ managed: `goauthentik.io/providers/oauth2/scope-${scope}` }, { parent: this }));
    const groups = new authentik.PropertyMappingProviderScope(`${name}-groups`, {
      name: `${args.displayName} verified groups`,
      scopeName: "profile",
      expression: 'return {"groups": [group.name for group in request.user.ak_groups.all()]}',
    }, child);
    const api = new authentik.PropertyMappingProviderScope(`${name}-api`, {
      name: `${args.displayName} API audience`,
      scopeName: "inbox:api",
      // Authentik also merges this into ID tokens; audience alone never establishes API token type.
      expression: `return {"aud": ${JSON.stringify(`${args.slug}-api`)}}`,
    }, child);
    const provider = new authentik.ProviderOauth2(`${name}-provider`, {
      name: args.displayName,
      clientId: args.slug,
      clientSecret: secret.result,
      clientType: "confidential",
      issuerMode: "per_provider",
      subMode: "user_uuid",
      includeClaimsInIdToken: true,
      authorizationFlow: authorizationFlow.id,
      invalidationFlow: invalidationFlow.id,
      allowedRedirectUris: ["/auth/callback", "/oauth/oidc/callback"].map((path) => ({
        matching_mode: "strict", url: `${args.publicUrl}${path}`,
      })),
      accessTokenValidity: "minutes=10",
      refreshTokenValidity: "days=30",
      signingKey: args.signingKeyId,
      propertyMappings: [...mappings.map((mapping) => mapping.id), groups.propertyMappingProviderScopeId, api.propertyMappingProviderScopeId],
    }, child);
    new authentik.Application(`${name}-application`, {
      name: args.displayName,
      slug: args.slug,
      protocolProvider: provider.providerOauth2Id.apply((id) => Number.parseInt(id, 10)),
      metaLaunchUrl: args.publicUrl,
    }, child);
    this.clientId = pulumi.output(args.slug);
    this.clientSecret = secret.result;
    this.issuer = pulumi.output(`${args.issuerBaseUrl}/application/o/${args.slug}/`);
    this.registerOutputs({ clientId: this.clientId, clientSecret: this.clientSecret, issuer: this.issuer });
  }
}
