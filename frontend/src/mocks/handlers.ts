import {http, HttpResponse} from "msw";
import {mockAccounts, mockConnections, mockHoldings, mockTransactions} from "./data.ts";
import type {OpsFiMetadata} from "../api/data-contracts.ts";

export const handlers = [
    http.get("*/yodlee-ops/api/v1/fimetadata", ({ request }) => {
        const url = new URL(request.url);
        const subject = url.searchParams.get("subject");

        const makeResponse = (data: OpsFiMetadata[]) =>
            HttpResponse.json({ opsFiMetadata: data, cursor: "" }, { status: 200});

        switch (subject) {
        case "connections":
            return makeResponse(mockConnections);
        case "accounts":
            return makeResponse(mockAccounts);
        case "holdings":
            return makeResponse(mockHoldings);
        case "transactions":
            return makeResponse(mockTransactions);
        default:
            return makeResponse([]);
        }
    }),
];