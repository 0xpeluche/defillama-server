// https://api.coingecko.com/api/v3/asset_platforms
export const chainToCoingeckoId = {
  shibarium: "shibarium",
  bsc: "binance-smart-chain",
  ethereum: "ethereum",
  polygon: "polygon-pos",
  avax: "avalanche",
  fantom: "fantom",
  xdai: "xdai",
  heco: "huobi-token",
  okexchain: "okex-chain",
  harmony: "harmony-shard-0",
  kcc: "kucoin-community-chain",
  celo: "celo",
  arbitrum: "arbitrum-one",
  iotex: "iotex",
  moonriver: "moonriver",
  solana: "solana",
  terra: "terra",
  tron: "tron",
  waves: "waves",
  klaytn: "klay-token",
  osmosis: "osmosis",
  kava: "kava",
  icon: "icon",
  optimism: "optimistic-ethereum",
  eos: "eos",
  secret: "secret",
  rsk: "rootstock",
  neo: "neo",
  // tezos: "tezos",
  wan: "wanchain",
  ontology: "ontology",
  algorand: "algorand",
  zilliqa: "zilliqa",
  kardia: "kardiachain",
  cronos: "cronos",
  aurora: "aurora",
  boba: "boba",
  metis: "metis-andromeda",
  telos: "telos",
  moonbeam: "moonbeam",
  meter: "meter",
  sx: "sx-network",
  velas: "velas",
  milkomeda: "milkomeda-cardano",
  aptos: "aptos",
  hydra: "hydra",
  era: "zksync",
  energi: "energi",
  smartbch: "smartbch",
  near: "near-protocol",
  bitgert: "bitgert",
  cardano: "cardano",
  sora: "sora",
  conflux: "conflux",
  syscoin: "syscoin",
  elrond: "elrond",
  astar: "astar",
  evmos: "evmos",
  core: "core",
  songbird: "songbird",
  bitci: "Bitcichain",
  pulse: "pulsechain",
  oasis: "oasis",
  fuse: "fuse",
  theta: "theta",
  elastos: "elastos",
  linea: "linea",
  oas: "oasys",
  canto: "canto",
  base: "base",
  factom: "factom",
  bittorrent: "bittorrent",
  thundercore: "thundercore",
  polygon_zkevm: "polygon-zkevm",
  pom: "proof-of-memes",
  arbitrum_nova: "arbitrum-nova",
  step: "step-network",
  dfk: "defi-kingdoms-blockchain",
  csc: "coinex-smart-chain",
  bitkub: "bitkub-chain",
  exn: "exosama",
  findora: "findora",
  tomochain: "tomochain",
  ethpow: "ethereumpow",
  tenet: "tenet",
  cube: "cube",
  onus: "onus",
  rollux: "rollux",
  mantle: "mantle",
  gochain: "gochain",
  callisto: "callisto",
  eos_evm: "eos-evm",
  neon_evm: "neon-evm",
  wemix: "wemix-network",
  tombchain: "tombchain",
  shiden: "shiden network",
  flare: "flare-network",
  functionx: "function-x",
  beam: "beam",
  blast: "blast",
  mode: "mode",
  scroll: "scroll",
  bitrock: "bitrock",
  polkadot: "polkadot",
  juno: "juno",
  injective: "injective",
  immutable: "immutable",
  xai: "xai",
  merlin: "merlin-chain",
  sei: "sei-network",
  radix: "radix",
  filecoin: "filecoin",
  zeta: "zetachain",
  libre: "libre",
  zkfair: "zkfair",
  starknet: "starknet",
  icp: "internet-computer",
  archway: "archway",
  migaloo: "migaloo",
  dogechain: "dogechain",
  acala: "acala",
  omax: "omax",
  nuls: "nuls",
  kujira: "kujira",
  fraxtal: "fraxtal",
  map: "map-protocol",
  zora: "zora-network",
  dydx: "dydx",
  manta: "manta-pacific",
  bouncebit: "bouncebit",
  taiko: "taiko",
  genesys: "genesys-network",
  lukso: "lukso",
  sanko: "sanko",
  massa: "massa",
  etherlink: "etherlink",
  endurance: "endurance",
  bitlayer: "bitlayer",
  bob: "bob-network",
  xlayer: "x-layer",
  planq: "planq-network",
  bsquared: "bsquared-network",
  nibiru: "nibiru",
  // hyperliquid: "hyperliquid",  // this is not evm?
  ancient8: "ancient8",
  degen: "degen",
  cronos_zkevm: "cronos-zkevm",
  iotaevm: "iota-evm",
  real: "re-al",
  eclipse: "eclipse",
  kusama: "kusama",
  empire: "empire",
  vite: "vite",
  hoo: "hoo",
  neutron: "neutron",
  celestia: "celestia",
  aura: "aura-network",
  echelon: "echelon",
  ton: "the-open-network",
  alephium: "alephium",
  berachain: "berachain",
  apechain: "apechain",
  boba_bnb: "boba-bnb",
  sui: "sui",
  sonic: "sonic",
  abstract: "abstract",
  // dl: cg
  chz: "chiliz",
  // stellar: "stellar",
  stacks: "stacks",
  // cosmos: "cosmos",
  // xrp: "xrp",
  hedera: "hedera-hashgraph",
  bfc: "bifrost-network",
  occ: "edu-chain",
  wc: "world-chain",
  soneium: "soneium",
  // hyperliquid: "hyperevm",
  unichain: "unichain",
  ink: "ink",
  swellchain: "swellchain",
  plume_mainnet: "plume-network",
  hemi: "hemi",
  sty: "story",
  hydration: "hydration",
  stellar: "stellar",
  xrp: "xrp",
  corn: "corn",
  morph: "morph-l2",
  ronin: "ronin",
  zircuit: "zircuit",
  op_bnb: "opbnb",
  xcc: "cyber",
  sophon: "sophon",
  lisk: "lisk",
  move: "movement",
  tezos: "tezos",
  artela: "artela",
  kroma: "kroma",
  lrs: "larissa",
  lightlink_phoenix: "lightlink",
  ftn: "bahamut",
  ethereumclassic: "ethereum-classic",
  flow: "flow-evm",
  shimmer_evm: "shimmer_evm",
  sapphire: "oasis-sapphire",
  xdc: "xdc-network",
  q: "q-mainnet",
  islm: "haqq-network",
  zklink: "zklink-nova",
  gravity: "gravity-alpha",
  tara: "taraxa",
  europa: "skale",
  duckchain: "duckchain",
  mint: "mint",
  defiverse: "defiverse",
  meld: "meld",
  bevm: "bevm",
  laika: "laikachain",
  shido: "shido",
  sseed: "superseed",
  hela: "hela",
  lens: "lens",
  sxr: "sx-rollup",
  saga: "saga",
  inevm: "inevm",
  airdao: "airdao",
  goat: "goat",
  etn: "electroneum",
  godwoken_v1: "godwoken",
  defichain_evm: "defichain",
  unit0: "units-network",
  rss3_vsl: "rss3-vsl",
  area: "areon-network ",
  zero_network: "zero-network",
  astrzk: "astar-zkevm",
  tac: "tac", 
  btnx: "botanix"
};

export const cgPlatformtoChainId: { [key: string]: string } = Object.entries(chainToCoingeckoId).reduce(
  (acc: any, [chain, cgId]) => {
    acc[cgId] = chain;
    return acc;
  },
  {},
);

cgPlatformtoChainId["hyperevm"] = "hyperliquid";
cgPlatformtoChainId["sei-v2"] = "sei";
cgPlatformtoChainId["zilliqa-evm"] = "zilliqa";

export default chainToCoingeckoId;

// const fetch = require("node-fetch");
// async function generateNewObject() {
//   const res: { id: string }[] = await fetch(
//     "https://api.coingecko.com/api/v3/asset_platforms",
//   ).then((r: any) => r.json());

//   const currentChains = Object.values(chainToCoingeckoId);
//   let missing: string = ``;
//   res.map((r) => {
//     if (currentChains.includes(r.id)) return;
//     missing = `${missing} ${r.id},`;
//   });

//   console.log(missing);
// }
// generateNewObject();
// ts-node common/chainToCoingeckoId.ts
