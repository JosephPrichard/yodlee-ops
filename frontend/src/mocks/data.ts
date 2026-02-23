import type {OpsFiMetadata} from "../api/data-contracts.ts";

function baseDate(daysAgo: number) {
    return new Date(
        Date.now() - daysAgo * 24 * 60 * 60 * 1000
    ).toISOString();
}

export const mockConnections: OpsFiMetadata[] = Array.from(
    {length: 8},
    (_, i) => ({
        key: `p${i + 1}/${1000 + i}`,
        profileID: `p${i + 1}`,
        providerAccountID: String(1000 + i), // numeric but stored as string
        partyIDTypeCd: ["BANK", "BROKERAGE", "CREDIT", "PAYMENTS"][i % 4],
        accountID: "",
        holdingID: "",
        transactionID: "",
        lastModified: baseDate(i),
        lastUpdated: baseDate(i + 1)
    })
);

export const mockAccounts: OpsFiMetadata[] = Array.from(
    {length: 8},
    (_, i) => ({
        key: `p${i + 1}/${2000 + i}`,
        profileID: `p${i + 1}`,
        providerAccountID: String(2000 + i),
        partyIDTypeCd: "BANK",
        accountID: String(5000 + i),
        holdingID: "",
        transactionID: "",
        lastModified: baseDate(i + 2),
        lastUpdated: baseDate(i + 3)
    })
);

export const mockHoldings: OpsFiMetadata[] = Array.from(
    {length: 8},
    (_, i) => ({
        key: `p1/${3000 + i}`,
        profileID: "p1",
        providerAccountID: "3001",
        partyIDTypeCd: "BROKERAGE",
        accountID: "6000",
        holdingID: String(8000 + i),
        transactionID: "",
        lastModified: baseDate(i + 4),
        lastUpdated: baseDate(i + 5)
    })
);

export const mockTransactions: OpsFiMetadata[] = Array.from(
    {length: 8},
    (_, i) => ({
        key: `p1/${4000 + i}`,
        profileID: "p1",
        providerAccountID: "4001",
        partyIDTypeCd: "BANK",
        accountID: "7000",
        holdingID: "",
        transactionID: String(9000 + i),
        lastModified: baseDate(i + 6),
        lastUpdated: baseDate(i + 7)
    })
);