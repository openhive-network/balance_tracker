import React,{useState} from 'react'

export default function App() {
const [value,setValue] = useState('')

fetch("http://localhost:3000/rpc/find_matching_accounts", {
        method:"post",
        headers: {
            "Content-Type" : "application/json",
            "Prefer" : "params=single-object" 
        },
        body: JSON.stringify({partial_account_name: value })
    })
    .then(response=>response.json())
    .then(res=>console.log(value ? JSON.parse(res) : ''))


    return (
        <div style={{display:'flex',justifyContent:'center'}}>
            <input
             value={value} 
             onChange={(e)=>setValue(e.target.value)} 
             style={{width:'500px', height:'60px'}} 
             placeholder='search for account' 
             type='text'  />
        </div>
    )
}
