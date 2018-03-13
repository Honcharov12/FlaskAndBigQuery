function loadXMLDoc() {
    console.log("it works!");
    var req = new XMLHttpRequest();
    req.onreadystatechange = function() {
        if (req.readyState == 4) {
            if (req.status != 200) {
                console.log("some error occurred")
            }
            else {
                var response = JSON.parse(req.responseText);
                document.getElementById('myDiv').innerHTML = response.returnedData;
            }
        }
    };

    req.open('POST', '/ajax');
    req.setRequestHeader("Content-type", "application/x-www-form-urlencoded");
    var query = document.getElementById('query').value;
    var postVars = 'query='+query;
    req.send(postVars);

    return false;
}

document.getElementById("submitButton").addEventListener("click", loadXMLDoc);
