Papa.parse("data/dataset_macro_mercado_mensal.csv", {

download: true,
header: true,

complete: function(results) {

const data = results.data;

// remove linhas vazias
const filtered = data.filter(row => row.date);

// datas
const dates = filtered.map(row => row.date);

// séries
const ibovespa = filtered.map(row => parseFloat(row.ibovespa));
const usd = filtered.map(row => parseFloat(row.usd_brl));
const selic = filtered.map(row => parseFloat(row.selic_diaria));
const ipca = filtered.map(row => parseFloat(row.ipca));

// último valor
const last = filtered[filtered.length - 1];

document.getElementById("ibovespa_value").innerText =
Number(last.ibovespa).toLocaleString();

document.getElementById("usd_value").innerText =
Number(last.usd_brl).toFixed(2);

document.getElementById("selic_value").innerText =
Number(last.selic_diaria).toFixed(2) + "%";

document.getElementById("ipca_value").innerText =
Number(last.ipca).toFixed(2);


// IBOVESPA

new Chart(
document.getElementById("ibovespaChart"),
{
type: "line",
data: {
labels: dates,
datasets: [{
label: "Ibovespa",
data: ibovespa
}]
}
}
);


// USD

new Chart(
document.getElementById("usdChart"),
{
type: "line",
data: {
labels: dates,
datasets: [{
label: "USD/BRL",
data: usd
}]
}
}
);


// SELIC

new Chart(
document.getElementById("selicChart"),
{
type: "line",
data: {
labels: dates,
datasets: [{
label: "Selic",
data: selic
}]
}
}
);


// IPCA

new Chart(
document.getElementById("ipcaChart"),
{
type: "line",
data: {
labels: dates,
datasets: [{
label: "IPCA",
data: ipca
}]
}
}
);

}

});
