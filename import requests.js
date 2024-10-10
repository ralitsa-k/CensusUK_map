
url = "https://docs.google.com/document/d/e/2PACX-1vRMx5YQlZNa3ra8dYYxmv-QIQ3YJe8tbI3kqcuC7lQiZm-CSEznKfN_HYNSpoXcZIV3Y_O3YoUB1ecq/pub"
retrieve_and_print_google_doc_grid(url)

function loadData() {
  var url="https://docs.google.com/spreadsheet/pub?key=p_aHW5nOrj0VO2ZHTRRtqTQ&single=true&gid=0&range=A1&output=csv";
  xmlhttp=new XMLHttpRequest();
  xmlhttp.onreadystatechange = function() {
    if(xmlhttp.readyState == 4 && xmlhttp.status==200){
      document.getElementById("display").innerHTML = xmlhttp.responseText;
    }
  };
  xmlhttp.open("GET",url,true);
  xmlhttp.send(null);
}
