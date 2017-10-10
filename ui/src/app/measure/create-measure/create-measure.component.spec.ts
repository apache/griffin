import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CreateMeasureComponent } from './create-measure.component';

describe('CreateMeasureComponent', () => {
  let component: CreateMeasureComponent;
  let fixture: ComponentFixture<CreateMeasureComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CreateMeasureComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CreateMeasureComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
