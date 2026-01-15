/** @type {import('tailwindcss').Config} */
export default {
  darkMode: 'class',
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        // Keep existing cpln-* colors for backwards compatibility
        'cpln-navy': 'rgb(36 36 66)',
        'cpln-navy-light': 'rgb(45 45 85)',
        'cpln-navy-dark': 'rgb(26 26 46)',
        'cpln-cyan-navy-mix': 'rgb(32 53 119)',
        'cpln-cyan': 'rgb(21 99 255)',
        'cpln-navy-dark-cyan-20': 'rgb(25 41 88)', // 20% blend from navy-dark toward cyan
        'cpln-cyan-dark': 'rgb(13 79 214)',
        'cpln-orange': 'rgb(254 95 6)',
        'cpln-green': 'rgb(57 215 139)',
        'cpln-yellow': 'rgb(249 217 82)',

        // Override indigo with cpln-navy scale
        indigo: {
          50: 'rgb(242 242 252)',   // Very light navy (blue +10)
          100: 'rgb(230 230 248)',  // Light navy (blue +18)
          200: 'rgb(205 205 235)',  // Lighter navy (blue +30)
          300: 'rgb(175 175 215)',  // Light-medium navy (blue +40)
          400: 'rgb(145 145 195)',  // Medium navy (blue +50)
          500: 'rgb(115 115 175)',  // Medium navy (blue +60)
          600: 'rgb(80 80 140)',    // Medium-dark navy (blue +60)
          700: 'rgb(36 36 66)',     // cpln-navy (main) (blue +30)
          800: 'rgb(26 26 46)',     // cpln-navy-dark (blue +20)
          900: 'rgb(18 18 32)',     // Darker navy (blue +14)
          950: 'rgb(12 12 22)',     // Darkest navy (blue +10)
        },

        // Override blue with cpln-cyan scale
        blue: {
          50: 'rgb(240 248 255)',   // Very light cyan
          100: 'rgb(224 242 255)',  // Light cyan
          200: 'rgb(186 230 255)',  // Lighter cyan
          300: 'rgb(125 203 255)',  // Light-medium cyan
          400: 'rgb(66 165 255)',   // Medium cyan
          500: 'rgb(21 99 255)',    // cpln-cyan (main)
          600: 'rgb(13 79 214)',    // cpln-cyan-dark
          700: 'rgb(10 63 171)',    // Darker cyan
          800: 'rgb(8 47 128)',     // Dark cyan
          900: 'rgb(5 31 85)',      // Darker cyan
          950: 'rgb(3 19 51)',      // Darkest cyan
        },
      },
    },
  },
  plugins: [],
}
