; ModuleID = 'probe6.a82ad13803b46ea-cgu.0'
source_filename = "probe6.a82ad13803b46ea-cgu.0"
target datalayout = "e-m:o-p270:32:32-p271:32:32-p272:64:64-i64:64-i128:128-n32:64-S128-Fn32"
target triple = "arm64-apple-macosx11.0.0"

; core::f64::<impl f64>::is_subnormal
; Function Attrs: inlinehint uwtable
define internal zeroext i1 @"_ZN4core3f6421_$LT$impl$u20$f64$GT$12is_subnormal17h1afb0beaa2c5d292E"(double %self) unnamed_addr #0 {
start:
  %_2 = alloca [1 x i8], align 1
  %b = bitcast double %self to i64
  %_5 = and i64 %b, 4503599627370495
  %_6 = and i64 %b, 9218868437227405312
  %0 = icmp eq i64 %_5, 0
  br i1 %0, label %bb2, label %bb8

bb2:                                              ; preds = %start
  %1 = icmp eq i64 %_6, 9218868437227405312
  br i1 %1, label %bb7, label %bb9

bb8:                                              ; preds = %start
  switch i64 %_6, label %bb3 [
    i64 9218868437227405312, label %bb6
    i64 0, label %bb4
  ]

bb7:                                              ; preds = %bb2
  store i8 1, ptr %_2, align 1
  br label %bb1

bb9:                                              ; preds = %bb2
  switch i64 %_6, label %bb3 [
    i64 9218868437227405312, label %bb6
    i64 0, label %bb5
  ]

bb1:                                              ; preds = %bb3, %bb4, %bb6, %bb5, %bb7
  %2 = load i8, ptr %_2, align 1
  %_3 = zext i8 %2 to i64
  %_0 = icmp eq i64 %_3, 3
  ret i1 %_0

bb3:                                              ; preds = %bb8, %bb9
  store i8 4, ptr %_2, align 1
  br label %bb1

bb6:                                              ; preds = %bb8, %bb9
  store i8 0, ptr %_2, align 1
  br label %bb1

bb5:                                              ; preds = %bb9
  store i8 2, ptr %_2, align 1
  br label %bb1

bb4:                                              ; preds = %bb8
  store i8 3, ptr %_2, align 1
  br label %bb1
}

; probe6::probe
; Function Attrs: uwtable
define void @_ZN6probe65probe17hdd14f57af7242211E() unnamed_addr #1 {
start:
; call core::f64::<impl f64>::is_subnormal
  %_1 = call zeroext i1 @"_ZN4core3f6421_$LT$impl$u20$f64$GT$12is_subnormal17h1afb0beaa2c5d292E"(double 1.000000e+00) #2
  ret void
}

attributes #0 = { inlinehint uwtable "frame-pointer"="non-leaf" "probe-stack"="inline-asm" "target-cpu"="apple-m1" }
attributes #1 = { uwtable "frame-pointer"="non-leaf" "probe-stack"="inline-asm" "target-cpu"="apple-m1" }
attributes #2 = { inlinehint }

!llvm.module.flags = !{!0}
!llvm.ident = !{!1}

!0 = !{i32 8, !"PIC Level", i32 2}
!1 = !{!"rustc version 1.94.1 (e408947bf 2026-03-25) (Homebrew)"}
