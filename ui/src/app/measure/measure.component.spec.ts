import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MeasureComponent } from './measure.component';

describe('MeasureComponent', () => {
  let component: MeasureComponent;
  let fixture: ComponentFixture<MeasureComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MeasureComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MeasureComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
