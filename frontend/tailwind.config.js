/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        // Dashboard theme colors
        primary: '#0ea5e9',        // Cyan
        'slate': {
          850: '#1e293b',
          950: '#0f172a',
        }
      },
    },
  },
  plugins: [],
}
