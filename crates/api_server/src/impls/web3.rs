use anvil_zksync_api_decl::Web3NamespaceServer;
use jsonrpsee::core::RpcResult;

pub struct Web3Namespace;

impl Web3NamespaceServer for Web3Namespace {
    fn client_version(&self) -> RpcResult<String> {
        // Lead with `anvil/` so tooling that special-cases dev networks
        // (notably OpenZeppelin Upgrades' `isDevelopmentNetwork`, which checks
        // `clientVersion.split('/', 1)[0] === 'anvil'`) treats this as a local
        // dev chain and silently re-deploys cached implementations whose
        // bytecode no longer exists on a fresh chain.
        Ok(format!(
            "anvil/v{} (zkSync v2.0)",
            env!("CARGO_PKG_VERSION")
        ))
    }
}
