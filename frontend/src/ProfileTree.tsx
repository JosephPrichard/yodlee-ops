import React, {useEffect, useState} from "react";
import {FiSubject, type OpsFiMetadata} from "./api/data-contracts.ts";
import {useNavigate} from "react-router-dom";
import {type Column, DataGrid} from "react-data-grid";
import {Fimetadata} from "./api/Fimetadata.ts";

type FiMetadataRow = OpsFiMetadata & {
    children?: FiMetadataRow[];
};

type ProfileTreeProps = {
    profileIDs: string;
};

const connectionColumns: Column<FiMetadataRow>[] = [
    {key: "providerAccountID", name: "Provider Account ID"},
    {key: "partyIDTypeCd", name: "Party Type"},
    ...dateColumns()
];

const accountColumns: Column<FiMetadataRow>[] = [
    {key: "providerAccountID", name: "Provider Account ID"},
    {key: "partyIDTypeCd", name: "Party Type"},
    {key: "accountID", name: "Account ID"},
    ...dateColumns()
];

const holdingColumns: Column<FiMetadataRow>[] = [
    {key: "partyIDTypeCd", name: "Party Type"},
    {key: "accountID", name: "Account ID"},
    {key: "holdingID", name: "Holding ID"},
    ...dateColumns()
];

const transactionColumns: Column<FiMetadataRow>[] = [
    {key: "partyIDTypeCd", name: "Party Type"},
    {key: "accountID", name: "Account ID"},
    {key: "transactionID", name: "Transaction ID"},
    ...dateColumns()
];

function dateColumns(): Column<OpsFiMetadata>[] {
    return [
        {
            key: "lastModified",
            name: "Last Modified",
            renderCell: ({row}) =>
                new Date(row.lastModified).toLocaleString()
        },
        {
            key: "lastUpdated",
            name: "Last Updated",
            renderCell: ({row}) =>
                new Date(row.lastUpdated).toLocaleString()
        }
    ];
}

const fiMetadataApi = new Fimetadata({
    baseURL: "http://localhost:8080/yodlee-ops/api/v1",
});
// const fiMetadataApi = new Fimetadata({
//     baseURL: "/yodlee-ops/api/v1",
// });

export const ProfileTree: React.FC<ProfileTreeProps> = ({profileIDs}) => {
    const navigate = useNavigate();

    let [rootConnections, setRootConnections] = useState<FiMetadataRow[]>([]);

    useEffect(() => {
        fiMetadataApi.getFiMetadata({profileIDs, cursor: "", subject: FiSubject.Connections})
            .then(resp => {
                setRootConnections(resp.data.opsFiMetadata);
            });
    }, [profileIDs]);

    return (
        <React.Fragment>
            <div className="banner"></div>

            <div className="tree-container">
                <div className="form-tree-container">
                    <form
                        method="GET"
                        action=""
                        onSubmit={(e) => {
                            e.preventDefault();
                            const formData = new FormData(e.currentTarget);
                            const newProfileIDs = formData.get("profileIDs")?.toString();
                            navigate("/logs?profileIDs=" + newProfileIDs, {replace: true});
                        }}
                    >
                        <div className="form-element">
                            <label>
                                <span className="label-text">Profiles</span>
                                <input
                                    className="text-input"
                                    type="text"
                                    name="profileIDs"
                                    defaultValue={profileIDs}
                                />
                            </label>
                            <div className="label-tooltip">
                                *Enter PIDs to subscribe to, separated by commas
                            </div>
                        </div>

                        <button type="submit">Search</button>
                    </form>
                </div>

                <div className="profile-container">
                    <div className="table-container">
                        <DataGrid
                            columns={[
                                // {
                                //     key: 'expand',
                                //     name: '',
                                //     width: 50,
                                //     renderCell: ({ row }) =>
                                //         row.details ? (
                                //             <button onClick={() => toggleRow(row.id)}>
                                //                 {expanded.has(row.id) ? '▼' : '▶'}
                                //             </button>
                                //         ) : null
                                // },
                                ...connectionColumns
                            ]}
                            rows={rootConnections}
                        />
                    </div>
                </div>
            </div>
        </React.Fragment>
    );
}