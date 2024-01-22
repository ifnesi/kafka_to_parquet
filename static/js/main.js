const UPDATE_FLAG_FILE = "data/.flag";

function get_flag() {
    var result;
    $.get({
        url: UPDATE_FLAG_FILE,
        async: false,
        cache: false,
        success: function(data) {
            result = data;
        },
    });
    return result;
}

$(document).ready(function() {
    var update_flag_content;
    const update_flag_last_content = get_flag();
    setInterval(function() {
        update_flag_content = get_flag();
        if (update_flag_content != update_flag_last_content) {
            location.reload();
        }
    }, 500);
 });