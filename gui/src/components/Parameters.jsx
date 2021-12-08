// import React, { useState } from "react";
// import {
//   FormControl,
//   InputLabel,
//   TextField,
//   Select,
//   MenuItem,
// } from "@mui/material";

// export default function Parameters() {
//   const [currency, setCurrency] = useState("");

//   const handleChange = (e) => {
//     setCurrency(e.target.value);
//   };
//   return (
//     <div>
//       <FormControl style={{ width: "100px" }}>
//         <InputLabel id="demo-simple-select-label">Crypto</InputLabel>
//         <Select
//           labelId="demo-simple-select-label"
//           id="demo-simple-select"
//           value={currency}
//           label="Crypto"
//           onChange={handleChange}
//         >
//           <MenuItem value="Hive">Hive</MenuItem>
//           <MenuItem value="HBD">HBD</MenuItem>
//         </Select>
//       </FormControl>
//       <TextField id="outlined-basic" label="Start Block" variant="outlined" />
//       <TextField id="outlined-basic" label="End Block" variant="outlined" />
//       <TextField
//         id="outlined-basic"
//         label="Block Increment"
//         variant="outlined"
//       />
//     </div>
//   );
// }
