import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MeasureDetailComponent } from './measure-detail.component';

describe('MeasureDetailComponent', () => {
  let component: MeasureDetailComponent;
  let fixture: ComponentFixture<MeasureDetailComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MeasureDetailComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MeasureDetailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
