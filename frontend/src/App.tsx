import React from "react";
import { BrowserRouter as Router, Routes, Route, useSearchParams } from "react-router-dom";
import { LogsPage } from "./LogsPage";

const LogsPageWrapper: React.FC = () => {
    const [searchParams] = useSearchParams();

    const profileIDs = searchParams.get("profileIDs")?.toString() ?? "";
    let topics = searchParams.get("topics")?.toString() ?? "";

    if (topics === "") {
        topics = "connections,accounts,transactions,holdings";
    }

    return <LogsPage profileIDs={profileIDs} topics={topics} />;
};

export const App: React.FC = () => {
    return (
        <Router>
            <Routes>
                <Route path="/logs" element={<LogsPageWrapper />} />
                {/* Add other routes here if needed */}
            </Routes>
        </Router>
    );
};