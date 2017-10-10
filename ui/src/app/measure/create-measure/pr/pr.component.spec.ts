import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { PrComponent } from './pr.component';

describe('PrComponent', () => {
  let component: PrComponent;
  let fixture: ComponentFixture<PrComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ PrComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PrComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});
