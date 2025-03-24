

const factorial = (n) => { return n <= 0 ? 1 : n * factorial(n - 1); };
console.log(`Factorial of 5 is ${factorial(5)}`);
console.log(`Factorial of 5 is ${factorial(10)}`);
