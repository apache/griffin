import {XHRBackend, Http, RequestOptions} from "@angular/http";
import {HttpService} from "./http.service";
import { LoaderService } from './../loader/loader.service';

export function httpFactory(xhrBackend: XHRBackend, requestOptions: RequestOptions, loaderService: LoaderService): Http {
    return new HttpService(xhrBackend, requestOptions, loaderService);
}