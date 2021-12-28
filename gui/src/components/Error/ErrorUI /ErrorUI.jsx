import React from "react";
import styles from "./ErrorUI.module.css";

export default function ErrorUI() {
  return (
    <div className={styles.container}>
      <h1>Oops... Something went wrong !</h1>
      <p> Go to homepage and try again !</p>
      <a href="/">Homepage</a>
    </div>
  );
}
