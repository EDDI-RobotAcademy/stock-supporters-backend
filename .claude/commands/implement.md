# Command: /implement

## 목적

Behavior Backlog를 기반으로  
실제 동작하는 코드를 구현한다.

이 Command는 Backlog의

- Backlog Title
- Success Criteria
- Todo

를 분석하여 **구현 코드를 생성한다.**

---

## 사용 방법

```
/implement
<Backlog 내용>
```

또는

```
/implement <Backlog Title>
```

---

## 동작 규칙

1. Backlog Title을 읽는다
2. Success Criteria를 분석한다
3. Todo를 기반으로 구현 범위를 결정한다
4. 실제 동작 가능한 코드를 생성한다

---

## 코드 생성 규칙

코드는 다음 기준을 따른다.

- 실제 실행 가능한 코드여야 한다
- 의존성 라이브러리를 명확히 명시한다
- 파일 구조를 포함한다
- 코드 설명을 포함한다

---

## 출력 형식

출력은 다음 구조를 따른다.

### Project Structure

프로젝트 파일 구조

### Implementation

실제 코드 구현

### Explanation

코드 설명