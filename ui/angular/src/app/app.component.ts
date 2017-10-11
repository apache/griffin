/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
import { Component ,Directive,ViewContainerRef} from '@angular/core';
// import { RouterModule, Routes } from '@angular/router';
import { Router} from "@angular/router";
import { HttpClient} from '@angular/common/http';
import * as $ from 'jquery';
import {ServiceService} from './service/service.service';

// import jQuery from 'jquery';

// import  'bootstrap/dist/js/bootstrap.min.js';


@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css'],
  providers:[ServiceService]
})
export class AppComponent {
  title = 'app';
  ntAccount : string;
  // ntAccount = "test";
  timestamp:Date;
  onResize(event){
   this.resizeMainWindow();

  }
  results:any;
  fullName ="hello";
  ngOnInit(){
    this.ntAccount = this.getCookie("ntAccount");
    this.fullName = this.getCookie("fullName");
    this.timestamp = new Date();
  }
  constructor(private router:Router,private http:HttpClient,public serviceService:ServiceService){

  }
  setCookie(name, value, days){
    
    let expires;
        if (days) {
            var date = new Date();
            date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
            console.log(date);
            expires = "; expires=" + date.toUTCString();
      } else {
          expires = "";
      }
      // document.cookie = encodeURIComponent(name) + "=" + encodeURIComponent(value) + expires + "; path=/";
      document.cookie = name + "=" + value + expires + "; path=/";
  }

  getCookie(key) {
        console.log(document.cookie);
        var keyValue = document.cookie.match('(^|;) ?' + key + '=([^;]*)(;|$)');
        return keyValue ? keyValue[2] : null;
    }

  loginBtnWait() {
      $('#login-btn').addClass('disabled')
        .text('Logging in......');
    }

  loginBtnActive() {
        $('#login-btn').removeClass('disabled')
        .text('Log in');
    }

  showLoginFailed() {
      $('#loginMsg').show()
        .text('Login failed. Try again.');
    }
  resizeMainWindow(){
    $('#mainWindow').height(window.innerHeight-56-90);
  }
  logout(){
      this.ntAccount = undefined;
      this.setCookie('ntAccount', undefined, -1);
      this.setCookie('fullName', undefined, -1);
    // this.router.navigate(['/login']) ;
    // window.location.replace ('login.html');

   }
  submit(event){
    if(event.which == 13){//enter
        event.preventDefault();
        $('#login-btn').click();
        $('#login-btn').focus();
      }
  }
  focus($event){
    $('#loginMsg').hide();
  }

  login(){
      var name = $('input:eq(0)').val();
      var password = $('input:eq(1)').val();
      console.log(name);
      var loginUrl = 'http://localhost:8080/api/v1/login/authenticate';
      this.loginBtnWait();

      this.http   
      .post(loginUrl,{username:name, password:password})
      .subscribe(data => {
        this.results = data;
        if(this.results.status == 0)
          {//logon success
           if($('input:eq(2)').prop('checked')){
            this.setCookie('ntAccount', this.results.ntAccount, 30);
            this.setCookie('fullName', this.results.fullName, 30);
           }else
           {
              this.setCookie('ntAccount', this.results.ntAccount,0);
              this.setCookie('fullName', this.results.fullName,0);
           }
            this.loginBtnActive()
            window.location.replace('/');
          }
          else{
              this.showLoginFailed();
              this.loginBtnActive();
          };
      
    },
    err => {
          this.showLoginFailed();
          this.loginBtnActive();
    });

  }
}


