/*
	Copyright (c) 2016 eBay Software Foundation.
	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

	    http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/

$(document).ready(function() {
    $('input:eq(1)').keyup(function(evt){
      if(evt.which == 13){//enter
        evt.preventDefault();
        $('#login-btn').click();
        $('#login-btn').focus();
      }
    });

    $('input:eq(0)').focus(function(evt){
      $('#loginMsg').hide();
    });

    $('input:eq(1)').focus(function(evt){
      $('#loginMsg').hide();
    });

    $('#login-btn').click(function() {



        var name = $('input:eq(0)').val();
        var password = $('input:eq(1)').val();

       var loginUrl = '/api/v1/login/authenticate';



        loginBtnWait();
        $.ajax({
            type: 'POST',
            url: loginUrl,
            data: JSON.stringify({username:name, password:password}),
            contentType: 'application/json',
            dataType: 'json',
            success: function(data){
                console.log(data);
                if(data.status == 0){//logon success
                    //console.log($('input:eq(3)').val());
                    if($('input:eq(2)').prop('checked')){
                        setCookie('ntAccount', data.ntAccount, 30);
                        setCookie('fullName', data.fullName, 30);
                    }else{
                        setCookie('ntAccount', data.ntAccount);
                        setCookie('fullName', data.fullName);
                    }

                    loginBtnActive()
                    window.location.replace('/');
                }else{
                    showLoginFailed();
                    loginBtnActive();
                };
            },
            error: function(jqXHR, textStatus, errorThrown){
//                showLoginFailed();
//                loginBtnActive();
                //::TODO ignore login for test purpose
                setCookie('ntAccount', 'test', 30);
                setCookie('fullName', 'test', 30);
                window.location.replace('/');
                //::TODO ignore login for test purpose done
            }

        });
    });

    function setCookie(name, value, days){
        if (days) {
            var date = new Date();
            date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
            expires = "; expires=" + date.toGMTString();
	    } else {
	        expires = "";
	    }
	    document.cookie = encodeURIComponent(name) + "=" + encodeURIComponent(value) + expires + "; path=/";
	}

	function getCookie(key) {
        var keyValue = document.cookie.match('(^|;) ?' + key + '=([^;]*)(;|$)');
        return keyValue ? keyValue[2] : null;
    }

    function loginBtnWait() {
      $('#login-btn').addClass('disabled')
        .text('Logging in......');
    }

    function loginBtnActive() {
        $('#login-btn').removeClass('disabled')
        .text('Log in');
    }

    function showLoginFailed() {
      $('#loginMsg').show()
        .text('Login failed. Try again.');
    }
});
