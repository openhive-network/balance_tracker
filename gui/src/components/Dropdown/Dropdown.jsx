import React from "react";
import {
  List,
  ListItem,
  ListItemButton,
  ListItemText,
  Divider,
} from "@mui/material";
import styles from "./Dropdown.module.css";

export default function Dropdown({ names, value, setValue }) {
  return (
    <nav
      className={
        !names || !value
          ? styles["dropdown__container-hide"]
          : styles["dropdown__container-show"]
      }
      aria-label="secondary mailbox folders"
    >
      <List className={styles["dropdown__list"]}>
        {names &&
          names.map((name, index) => {
            function handleOnClickName() {
              setValue(name);
            }
            return (
              <div key={index}>
                <ListItem disablePadding>
                  <ListItemButton onClick={handleOnClickName}>
                    <ListItemText primary={name} />
                  </ListItemButton>
                </ListItem>
                <Divider />
              </div>
            );
          })}
      </List>
    </nav>
  );
}
