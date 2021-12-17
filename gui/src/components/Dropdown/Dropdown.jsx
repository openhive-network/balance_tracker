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
  const renderDropdown = () => {
    if (names === null) {
      return console.log("No Names");
    }
    if (!value || value === names[0]) {
      return styles["dropdown__container-hide"];
    }
    if (value.length >= 1) {
      return styles["dropdown__container-show"];
    }
  };
  return (
    <>
      <div className={renderDropdown()} aria-label="secondary mailbox folders">
        <List className={styles["dropdown__list"]}>
          {names ? (
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
            })
          ) : (
            <div className={styles.noNamesError}>
              <p>No Names</p>
            </div>
          )}
        </List>
      </div>
    </>
  );
}
