import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MydashboardComponent } from './mydashboard.component';

describe('MydashboardComponent', () => {
  let component: MydashboardComponent;
  let fixture: ComponentFixture<MydashboardComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MydashboardComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MydashboardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
