import React,{useState,useEffect} from 'react'

export default function App() {
const [value,setValue] = useState('')
const [names,setNames] = useState('')

const data = JSON.stringify({partial_account_name: value })



useEffect(()=>{
    fetch("http://localhost:3000/rpc/find_matching_accounts", {
        method:"post",
        headers: {
            "Content-Type" : "application/json",
            "Prefer" : "params=single-object" 
            },
        body: data,
    })
    .then(response=>response.json())
    .then(res=>setNames(JSON.parse(res)))
},[data])

    return (
        <div style={{display:'flex',justifyContent:'center'}}>
            <div>
            <input
             value={value} 
             onChange={(e)=>setValue(e.target.value)} 
             style={{width:'500px', height:'60px'}} 
             placeholder='search for account' 
             type='text'  />

            <div style={!value ?
                 {display:'none'} :
                {display:"flex",
                flexDirection:'column',
                overflow:'auto',
                width:'500px',
                maxHeight:'200px',
                border:'1px solid black'}}>
                   <ul style={{listStyle:'none'}}>
                       {names && names.map((name,index)=>
                       <li key={index}>
                           {name}
                        </li>
                       )}
                </ul>
            </div>
            </div>
        </div>
    )
}
