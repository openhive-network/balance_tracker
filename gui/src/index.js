import React from "react";
import ReactDOM from "react-dom";
import App from "./components/App/App";
import ErrorBoundary from "./components/Error/ErrorBoundary";

ReactDOM.render(
  <ErrorBoundary>
    <App />
  </ErrorBoundary>,
  document.getElementById("root")
);
