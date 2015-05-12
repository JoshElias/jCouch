var jCouch = require("./jCouch");

jCouch.get("spark", "UserSetting::RF2F40ZRB5T::activityTimeout::ideal", function(err, doc) {
	if(err) {
		console.log("Error getting doc");
	} else {
		console.log("Successfully retrieved document");
		console.log(doc);
	}
});