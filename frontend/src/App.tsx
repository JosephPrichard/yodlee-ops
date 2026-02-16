import React from "react";
import {BrowserRouter as Router, Routes, Route, useSearchParams, useNavigate} from "react-router-dom";
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

const HomePage: React.FC = () => {
    const navigate = useNavigate();

    const goToProfileTree = () => {

    }

    const goToLogs = () => {
        navigate("/logs");
    };

    return (
        <div style={{ display: "flex", justifyContent: "center", alignItems: "center", height: "100vh", flexDirection: "column" }}>
            <h1>Yodlee OPs Admin Dashboard</h1>
            <div className="button-container">
                <button onClick={goToProfileTree} style={{ padding: "10px 20px", fontSize: "16px", cursor: "pointer" }}>
                    Profile Tree
                </button>
                <button onClick={goToLogs} style={{ padding: "10px 20px", fontSize: "16px", cursor: "pointer" }}>
                    Log Stream
                </button>
            </div>
        </div>
    );
};

export const App: React.FC = () => {
    return (
        <Router>
            <Routes>
                <Route path="/" element={<HomePage />} />
                <Route path="/logs" element={<LogsPageWrapper />} />
            </Routes>
        </Router>
    );
};