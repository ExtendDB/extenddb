// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

// The metrics dashboard: line charts, latency drill-down, availability
// and CSV/JSON export. The page loads it as a file, because the
// console's Content-Security-Policy runs no inline script. The script
// tag's data-ops attribute lists the data plane operations.
const DATA_OPS = new Set(document.currentScript.dataset.ops.split(' '));
let currentWindow='Last5Minutes',customStart=null,customEnd=null,autoRefresh=null,selectedLatencyOp=null;
const chartData={};
function esc(s){const d=document.createElement('div');d.textContent=s;return d.innerHTML;}
function isDataOp(op){return DATA_OPS.has(op);}
function drawLine(id,labels,series,yLabel){
  const c=document.getElementById(id);if(!c)return;
  const ctx=c.getContext('2d'),w=c.width=c.parentElement.clientWidth-32,h=c.height=180;
  const P={l:50,r:10,t:10,b:25};ctx.clearRect(0,0,w,h);
  const all=series.flatMap(s=>s.values);
  if(!all.length){ctx.fillStyle='#999';ctx.font='12px sans-serif';ctx.fillText('No data',w/2-25,h/2);return;}
  const mx=Math.max(...all,0.001),mn=Math.min(...all,0),rng=mx-mn||1,pw=w-P.l-P.r,ph=h-P.t-P.b;
  ctx.strokeStyle='#e5e7eb';ctx.lineWidth=1;
  for(let i=0;i<=4;i++){const y=P.t+ph*(1-i/4);ctx.beginPath();ctx.moveTo(P.l,y);ctx.lineTo(w-P.r,y);ctx.stroke();ctx.fillStyle='#999';ctx.font='10px sans-serif';ctx.textAlign='right';ctx.fillText((mn+rng*i/4).toFixed(1),P.l-4,y+3);}
  ctx.textAlign='center';ctx.fillStyle='#666';ctx.font='10px sans-serif';
  const st=Math.max(1,Math.floor(labels.length/6));
  for(let i=0;i<labels.length;i+=st){const x=P.l+(i/Math.max(labels.length-1,1))*pw;ctx.fillText(labels[i],x,h-4);}
  for(const s of series){ctx.strokeStyle=s.color;ctx.lineWidth=s.width||2;ctx.beginPath();for(let i=0;i<s.values.length;i++){const x=P.l+(i/Math.max(s.values.length-1,1))*pw,y=P.t+ph*(1-(s.values[i]-mn)/rng);if(i===0)ctx.moveTo(x,y);else ctx.lineTo(x,y);}ctx.stroke();}
}
function buildUrl(){if(customStart&&customEnd)return'/console/metrics-data?start='+encodeURIComponent(customStart)+'&end='+encodeURIComponent(customEnd);return'/console/metrics-data?window='+currentWindow;}
function updateWindowLinks(){document.querySelectorAll('.window-link').forEach(a=>{if(a.dataset.window===currentWindow&&!customStart){a.style.background='#2563eb';a.style.color='#fff';}else{a.style.background='';a.style.color='';}});}
function tsLabel(t){const d=new Date(t);return d.getHours().toString().padStart(2,'0')+':'+d.getMinutes().toString().padStart(2,'0');}
function aggBuckets(bkts,metric,opF){
  const f=bkts.filter(b=>{if(b.metric!==metric)return false;const o=(b.dimensions||[]).find(d=>d.Operation);return!opF||!o||o.Operation===opF;});
  const m={};f.forEach(b=>{if(!m[b.timestamp])m[b.timestamp]={sum:0,count:0,min:Infinity,max:-Infinity};m[b.timestamp].sum+=b.sum;m[b.timestamp].count+=b.count;m[b.timestamp].min=Math.min(m[b.timestamp].min,b.min);m[b.timestamp].max=Math.max(m[b.timestamp].max,b.max);});return m;
}
function buildLatencyOps(metrics){
  const el=document.getElementById('latency-ops');if(!el)return;
  const ops=new Set();metrics.forEach(x=>{if(x.metric!=='SuccessfulRequestLatency')return;const o=(x.dimensions||[]).find(d=>d.Operation);if(o&&isDataOp(o.Operation))ops.add(o.Operation);});
  el.innerHTML='';const ab=document.createElement('button');ab.textContent='All';ab.className='btn btn-sm'+(!selectedLatencyOp?' btn-primary':'');ab.onclick=()=>{selectedLatencyOp=null;refresh();};el.appendChild(ab);
  [...ops].sort().forEach(op=>{const b=document.createElement('button');b.textContent=op;b.className='btn btn-sm'+(selectedLatencyOp===op?' btn-primary':'');b.onclick=()=>{selectedLatencyOp=op;refresh();};el.appendChild(b);});
}
function calcAvail(bkts){
  const m={};bkts.forEach(b=>{const o=(b.dimensions||[]).find(d=>d.Operation);if(!o||!isDataOp(o.Operation))return;if(!m[b.timestamp])m[b.timestamp]={ok:0,err:0};if(b.metric==='SuccessfulRequestLatency')m[b.timestamp].ok+=b.count;if(b.metric==='UserErrors'||b.metric==='SystemErrors')m[b.timestamp].err+=b.sum;});
  const s=Object.keys(m).sort();return{labels:s.map(tsLabel),values:s.map(t=>{const tot=m[t].ok+m[t].err;return tot>0?((1-m[t].err/tot)*100):100;})};
}
const SEG_COLORS={auth:'#3b82f6',authz:'#8b5cf6',throttle:'#6b7280',dispatch:'#059669',response:'#f59e0b'};
function renderSegments(segs){
  const el=document.getElementById('segment-bars');if(!el)return;
  if(!segs||!segs.length){el.innerHTML='<p style=\'color:#999;font-size:0.85rem\'>No segment data yet.</p>';return;}
  let h='<table style=\'width:100%;border-collapse:collapse;font-size:0.85rem\'><thead><tr><th style=\'text-align:left;padding:4px\'>Operation</th><th style=\'text-align:left;padding:4px\'>Count</th><th style=\'text-align:left;padding:4px\'>Breakdown</th><th style=\'text-align:right;padding:4px\'>Total</th></tr></thead><tbody>';
  segs.sort((a,b)=>a.operation.localeCompare(b.operation));
  for(const s of segs){
    const a=s.avg,tot=a.total_us||1;
    const parts=[{n:'auth',v:a.auth_us},{n:'authz',v:a.authz_us},{n:'throttle',v:a.throttle_us},{n:'dispatch',v:a.dispatch_us},{n:'response',v:a.response_us}];
    let bar='<div style=\'display:flex;height:20px;width:100%;border-radius:3px;overflow:hidden\' title=\'';
    bar+=parts.map(p=>p.n+': '+(p.v/1000).toFixed(1)+'ms ('+(p.v/tot*100).toFixed(0)+'%)').join(', ');
    bar+='\'>';
    for(const p of parts){const pct=p.v/tot*100;if(pct>0.5)bar+='<div style=\'width:'+pct+'%;background:'+SEG_COLORS[p.n]+';min-width:2px\' title=\''+p.n+': '+(p.v/1000).toFixed(1)+'ms\'></div>';}
    bar+='</div>';
    h+='<tr><td style=\'padding:4px;font-family:monospace\'>'+esc(s.operation)+'</td><td style=\'padding:4px\'>'+s.count+'</td><td style=\'padding:4px\'>'+bar+'</td><td style=\'padding:4px;text-align:right;font-family:monospace\'>'+(tot/1000).toFixed(1)+'ms</td></tr>';
  }
  h+='</tbody></table>';
  h+='<div style=\'margin-top:0.5rem;font-size:0.75rem;display:flex;gap:1rem\'>';
  for(const[n,c] of Object.entries(SEG_COLORS))h+='<span><span style=\'display:inline-block;width:12px;height:12px;background:'+c+';border-radius:2px;vertical-align:middle\'></span> '+n+'</span>';
  h+='</div>';
  el.innerHTML=h;
}
async function refresh(){
  try{
    const r=await fetch(buildUrl()),d=await r.json(),m=d.metrics||[],bkts=d.buckets||[],segs=d.segments||[];
    document.getElementById('data-source').textContent=d.source||'memory';
    buildLatencyOps(m);
    renderSegments(segs);
    const body=document.getElementById('metrics-body');
    body.innerHTML=m.map(x=>{const dims=x.dimensions||[],acct=(dims.find(d=>d.AccountId)||{}).AccountId||'',tbl=(dims.find(d=>d.TableName)||{}).TableName||'',op=(dims.find(d=>d.Operation)||{}).Operation||'',pl=isDataOp(op)?'Data':'Control';return'<tr><td>'+esc(x.metric)+'</td><td>'+esc(acct)+'</td><td>'+esc(tbl)+'</td><td>'+esc(op)+'</td><td>'+pl+'</td><td>'+x.sum.toFixed(2)+'</td><td>'+x.count+'</td><td>'+x.min.toFixed(2)+'</td><td>'+x.max.toFixed(2)+'</td></tr>';}).join('');
    if(bkts.length>0)renderBkt(bkts);else{['c-latency','c-availability','c-rcu','c-wcu','c-cp-latency','c-cp-errors'].forEach(id=>drawLine(id,[],[]));};
  }catch(e){console.error('metrics fetch failed',e);}
}
function dpLatFilter(b){if(b.metric!=='SuccessfulRequestLatency')return false;const o=(b.dimensions||[]).find(d=>d.Operation);return o&&isDataOp(o.Operation)&&(!selectedLatencyOp||o.Operation===selectedLatencyOp);}
function cpLatFilter(b){if(b.metric!=='SuccessfulRequestLatency')return false;const o=(b.dimensions||[]).find(d=>d.Operation);return o&&!isDataOp(o.Operation);}
function cpErrFilter(b){if(b.metric!=='UserErrors'&&b.metric!=='SystemErrors')return false;const o=(b.dimensions||[]).find(d=>d.Operation);return o&&!isDataOp(o.Operation);}
function aggByTs(bkts,filter,valFn){
  const m={};bkts.filter(filter).forEach(b=>{if(!m[b.timestamp])m[b.timestamp]={sum:0,count:0,max:-Infinity};m[b.timestamp].sum+=b.sum;m[b.timestamp].count+=b.count;m[b.timestamp].max=Math.max(m[b.timestamp].max,b.max);});
  const s=Object.keys(m).sort(),l=s.map(tsLabel),v=s.map(t=>valFn(m[t]));return{labels:l,values:v};
}
function renderBkt(bkts){
  const lat=aggByTs(bkts,dpLatFilter,a=>a.count>0?a.sum/a.count:0);
  const p99=aggByTs(bkts,dpLatFilter,a=>a.max);
  chartData.latency={labels:lat.labels,series:{avg:lat.values,p50:lat.values,p99:p99.values}};
  drawLine('c-latency',lat.labels,[{values:lat.values,color:'#2563eb',width:2},{values:lat.values,color:'#93c5fd',width:1},{values:p99.values,color:'#f97316',width:1}]);
  const av=calcAvail(bkts);chartData.availability={labels:av.labels,series:{availability:av.values}};
  drawLine('c-availability',av.labels,[{values:av.values,color:'#059669',width:2}]);
  const rcu=aggByTs(bkts,b=>b.metric==='ConsumedReadCapacityUnits',a=>a.sum);chartData.rcu={labels:rcu.labels,series:{rcu:rcu.values}};
  drawLine('c-rcu',rcu.labels,[{values:rcu.values,color:'#059669',width:2}]);
  const wcu=aggByTs(bkts,b=>b.metric==='ConsumedWriteCapacityUnits',a=>a.sum);chartData.wcu={labels:wcu.labels,series:{wcu:wcu.values}};
  drawLine('c-wcu',wcu.labels,[{values:wcu.values,color:'#d97706',width:2}]);
  const cpl=aggByTs(bkts,cpLatFilter,a=>a.count>0?a.sum/a.count:0);chartData['cp-latency']={labels:cpl.labels,series:{avg:cpl.values}};
  drawLine('c-cp-latency',cpl.labels,[{values:cpl.values,color:'#7c3aed',width:2}]);
  const cpe=aggByTs(bkts,cpErrFilter,a=>a.sum);chartData['cp-errors']={labels:cpe.labels,series:{errors:cpe.values}};
  drawLine('c-cp-errors',cpe.labels,[{values:cpe.values,color:'#dc2626',width:2}]);
}
function exportData(name,fmt){
  const d=chartData[name];if(!d||!d.labels){alert('No data to export');return;}
  let c,mime,ext;
  if(fmt==='csv'){const sn=Object.keys(d.series);c='timestamp,'+sn.join(',')+'\n'+d.labels.map((l,i)=>l+','+sn.map(s=>d.series[s][i]).join(',')).join('\n');mime='text/csv';ext='csv';}
  else{c=JSON.stringify(d.labels.map((l,i)=>{const r={timestamp:l};for(const s of Object.keys(d.series))r[s]=d.series[s][i];return r;}),null,2);mime='application/json';ext='json';}
  const b=new Blob([c],{type:mime}),a=document.createElement('a');a.href=URL.createObjectURL(b);a.download='metrics-'+name+'.'+ext;a.click();URL.revokeObjectURL(a.href);
}
document.querySelectorAll('.window-link').forEach(a=>{a.addEventListener('click',e=>{e.preventDefault();currentWindow=a.dataset.window;customStart=null;customEnd=null;updateWindowLinks();refresh();clearInterval(autoRefresh);if(currentWindow==='Last5Minutes')autoRefresh=setInterval(refresh,10000);});});
document.getElementById('custom-go').addEventListener('click',()=>{const s=document.getElementById('custom-start').value,e=document.getElementById('custom-end').value;if(s&&e){customStart=new Date(s).toISOString();customEnd=new Date(e).toISOString();currentWindow='';updateWindowLinks();clearInterval(autoRefresh);refresh();}});
document.querySelectorAll('.export-btn').forEach(b=>{b.addEventListener('click',()=>exportData(b.dataset.chart,b.dataset.format));});
const nowLocal=new Date(),hourAgo=new Date(nowLocal.getTime()-3600000);
document.getElementById('custom-end').value=nowLocal.toISOString().slice(0,16);
document.getElementById('custom-start').value=hourAgo.toISOString().slice(0,16);
refresh();autoRefresh=setInterval(refresh,10000);
