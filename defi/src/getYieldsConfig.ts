import protocols from "./protocols/data";
import sluggify from "./utils/sluggify";

export function getYieldsConfig() {
    const final = {} as any
    protocols.forEach((p) => {
        final[sluggify(p)] = {
            name: p.name,
            audits: p.audits,
            audit_links: p.audit_links,
            url: p.url,
            twitter: p.twitter,
            category: p.category,
            symbol: p.symbol,
        }
    })
    return {
        protocols: final
    }
}

