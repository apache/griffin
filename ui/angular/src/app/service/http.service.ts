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

    post(url: string, body: string, options?: RequestOptionsArgs): Observable<Response> {
        return this.intercept(super.post(url, body, this.getRequestOptionArgs(options)));
    }

    put(url: string, body: string, options?: RequestOptionsArgs): Observable<Response> {
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
        // options.headers.append('Content-Type', 'application/json');
        return options;
    }

    intercept(observable: Observable<Response>): Observable<Response> {
        this.showLoader();
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
                this.hideLoader();              
            });
          });
        }

        private showLoader(): void {
            console.log("In-showLoader: showLoader");  
            this.loaderService.show();
        }
    
        private hideLoader(): void {
            this.loaderService.hide();
        }

}