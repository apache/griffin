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
import {async, ComponentFixture, TestBed, inject} from '@angular/core/testing';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';

import { AppModule } from '../app.module';
import { ServiceService } from '../service/service.service';
import {MetricComponent} from './metric.component';
import { LoaderService } from '../loader/loader.service';
import { ChartService } from '../service/chart.service';

describe('MetricComponent', () => {
  let component: MetricComponent;
  let fixture: ComponentFixture<MetricComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [ HttpClientTestingModule, AppModule ],
      declarations: [],
      providers: [ LoaderService, ServiceService, ChartService ]
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MetricComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it(
    'should be created', 
    inject(
      [HttpTestingController, ServiceService],
      (httpMock: HttpTestingController, serviceService: ServiceService) => {

        const req = httpMock.expectOne( serviceService.config.uri.dashboard );
        expect( req.request.method ).toBe("GET");
        req.flush( [] );

        expect(component).toBeTruthy();
      }
    )
  );
});
