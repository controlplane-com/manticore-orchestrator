import { ReactNode } from 'react';
import { Navbar } from './Navbar';

interface LayoutProps {
  children: ReactNode;
}

export const Layout = ({ children }: LayoutProps) => {
  return (
    <div className="min-h-screen bg-gray-50 dark:bg-stone-900 flex flex-col">
      <Navbar />

      <main className="flex-1 py-6 px-4 sm:px-6 lg:px-8">
        {children}
      </main>

      <footer className="py-6 bg-stone-900 text-gray-400 text-center text-sm">
        <p>Manticore Search - Control Plane</p>
      </footer>
    </div>
  );
};
