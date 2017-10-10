import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AcComponent } from './ac.component';

describe('AcComponent', () => {
  let component: AcComponent;
  let fixture: ComponentFixture<AcComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AcComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AcComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
