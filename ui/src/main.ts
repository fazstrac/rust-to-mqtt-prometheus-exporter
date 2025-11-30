async function listMappings(){
  const res = await fetch('/mapping');
  if(!res.ok){ console.error('failed to load mappings'); return }
  const data = await res.json();
  const tbody = document.querySelector('#mappingsTable tbody')!;
  tbody.innerHTML = '';
  (data as any[]).forEach(m=>{
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${m.manufacturer}</td><td>${m.sensor_id}</td><td>${m.name}</td>`;
    tbody.appendChild(tr);
  });
}

document.getElementById('mappingForm')!.addEventListener('submit', async (e)=>{
  e.preventDefault();
  const sensor_id = (document.getElementById('sensor_id') as HTMLInputElement).value;
  const manufacturer = (document.getElementById('manufacturer') as HTMLInputElement).value;
  const name = (document.getElementById('name') as HTMLInputElement).value;
  const payload = { sensor_id, manufacturer, name };
  const res = await fetch('/mapping', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
  if(res.status===201){
    await listMappings();
    (document.getElementById('mappingForm') as HTMLFormElement).reset();
  } else {
    alert('Failed to save mapping');
  }
});

listMappings();
