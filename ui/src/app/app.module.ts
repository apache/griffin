import { BrowserModule } from '@angular/platform-browser';
import {NgModule} from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { HttpClientModule} from '@angular/common/http';

import { Ng2SmartTableModule } from 'ng2-smart-table';
import {DataTableModule} from "angular2-datatable";
import { TreeModule } from 'angular-tree-component';
import { BrowserAnimationsModule} from '@angular/platform-browser/animations';
import { AngularEchartsModule } from 'ngx-echarts';
import { MdDatepickerModule, MdNativeDateModule} from '@angular/material';

import {ToasterModule, ToasterService} from 'angular2-toaster';
import { FormsModule } from '@angular/forms';
import { AppComponent } from './app.component';
import { MeasureComponent } from './measure/measure.component';
import { JobComponent } from './job/job.component';
import { SidebarComponent } from './sidebar/sidebar.component';
import { HealthComponent } from './health/health.component';
import { MydashboardComponent } from './mydashboard/mydashboard.component';
import { CreateMeasureComponent } from './measure/create-measure/create-measure.component';
import { MeasureDetailComponent } from './measure/measure-detail/measure-detail.component';
import { MetricComponent } from './metric/metric.component';
import { DetailMetricComponent } from './metric/detail-metric/detail-metric.component';
import { DataassetComponent } from './dataasset/dataasset.component';
import { CreateJobComponent } from './job/create-job/create-job.component';
import { AcComponent} from './measure/create-measure/ac/ac.component';
import { PrComponent } from './measure/create-measure/pr/pr.component';
import { LoginComponent } from './login/login.component';



const appRoutes: Routes = [
  { 
    path: 'health',
    component: HealthComponent 
  },
  { 
    path: 'measures', 
    component: MeasureComponent 
  },
  { 
    path: 'measure/:id',
    component: MeasureDetailComponent 
  },
  { 
    path: 'mydashboard', 
    component: MetricComponent 
  },
  {
    path: 'jobs',
    component: JobComponent,
  },
  {
    path: 'createjob',
    component: CreateJobComponent,
    
  },
  {
    path: 'createmeasure',
    component:CreateMeasureComponent
  },
  {
    path: 'createmeasureac',
    component:AcComponent
  },
    {
    path: 'createmeasurepr',
    component:PrComponent
  },
  {
    path: 'detailed/:name',
    component:DetailMetricComponent
  },
  {
    path: 'dataassets',
    component:DataassetComponent
  },
  {
    path: 'metrics',
    component:MetricComponent
  },
  { 
    path: '',
    redirectTo: 'health',
    pathMatch: 'full'
  },
  { 
    path: 'login',
    component:LoginComponent
  },
  // { 
  //   path: '**',
  //   component: AppComponent 
  // }

];

@NgModule({
  declarations: [
    AppComponent,
    MeasureComponent,
    JobComponent,
    SidebarComponent,
    HealthComponent,
    MydashboardComponent,
    CreateMeasureComponent,
    MeasureDetailComponent,
    MetricComponent,
    DetailMetricComponent,
    DataassetComponent,
    CreateJobComponent,
    AcComponent,
    PrComponent,
    LoginComponent,
  ],
  imports: [
    BrowserModule,
    HttpClientModule,
    Ng2SmartTableModule,
    TreeModule,
    BrowserAnimationsModule,
    ToasterModule,
    FormsModule,
    AngularEchartsModule,
    DataTableModule,    
    RouterModule.forRoot(
      appRoutes,
      { enableTracing: false } // <-- debugging purposes only
    ),
    MdNativeDateModule,
    MdDatepickerModule
    
    
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
