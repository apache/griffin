import { Component, OnInit } from '@angular/core';
import { FormControl} from '@angular/forms';
import { FormsModule } from '@angular/forms';

import { TREE_ACTIONS, KEYS, IActionMapping, ITreeOptions } from 'angular-tree-component';
import { BrowserAnimationsModule} from '@angular/platform-browser/animations';
import { ToasterModule, ToasterService, ToasterConfig} from 'angular2-toaster';
import * as $ from 'jquery';
import { HttpClient} from '@angular/common/http';
import { Router} from "@angular/router";

@Component({
  selector: 'app-create-measure',
  templateUrl: './create-measure.component.html',
  styleUrls: ['./create-measure.component.css']
})
export class CreateMeasureComponent implements OnInit {

  constructor(private router:Router) { }


  ngOnInit() {
  	$('#panel-2 >.panel-body').css({height: $('#panel-1 >.panel-body').outerHeight() + $('#panel-1 >.panel-footer').outerHeight() - $('#panel-2 >.panel-footer').outerHeight()});
    $('#panel-4 >.panel-body').css({height: $('#panel-3 >.panel-body').outerHeight() + $('#panel-3 >.panel-footer').outerHeight() - $('#panel-4 >.panel-footer').outerHeight()});

  }
  click(type){
  	this.router.navigate(['/createmeasure'+type]);
  }


}


