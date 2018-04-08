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
import {Injectable} from "@angular/core";
import { XHRBackend, RequestOptions, Request, RequestOptionsArgs, Response, Http, Headers} from "@angular/http";
import {Observable} from "rxjs/Rx";
import { LoaderService } from "./../loader/loader.service";

@Injectable()
export class HttpService extends Http {    
    constructor( backend: XHRBackend, options: RequestOptions,
        private loaderService: LoaderService) {
        super(backend, options);
    }
    request(url: string | Request, options?: RequestOptionsArgs): Observable<Response> {
        return this.intercept(super.request(url, options));
    }

    get(url: string, options?: RequestOptionsArgs): Observable<Response> {
        return this.intercept(super.get(url,options));
    }

    post(url: string, body: any, options?: RequestOptionsArgs): Observable<Response> {
        return this.intercept(super.post(url, body, this.getRequestOptionArgs(options)));
    }

    put(url: string, body: any, options?: RequestOptionsArgs): Observable<Response> {
        return this.intercept(super.put(url, body, this.getRequestOptionArgs(options)));
    }

    delete(url: string, options?: RequestOptionsArgs): Observable<Response> {
        return this.intercept(super.delete(url, this.getRequestOptionArgs(options)));
    }

    private getRequestOptionArgs(options?: RequestOptionsArgs) : RequestOptionsArgs {
        if (options == null) {
            options = new RequestOptions();
        }
        if (options.headers == null) {
            options.headers = new Headers();
        }
        options.headers.append('Content-Type', 'application/json');
        return options;
    }

    intercept(observable: Observable<Response>): Observable<Response> {
        this.showLoading();
        console.log("In the intercept routine..");      
        return observable
          .catch(e => { console.log(e); return Observable.of(e); 
          })
          .do((res: Response) => {
            console.log("Response: " + res);  
            console.log("XXXXBegin... " + res);         
          }, (err: any) => {           
            console.log("Caught error: " + err);
            console.log("YYYYEnd... " );     
          })
          .finally(() => {
            console.log("Finally.. delaying, though.")
            var timer = Observable.timer(1000);
            timer.subscribe(t => { 
                console.log("YYYYEnd... " );   
                this.hideLoading();              
            });
          });
        }

        private showLoading(): void {
            this.loaderService.show();
        }
    
        private hideLoading(): void {
            this.loaderService.hide();
        }

}