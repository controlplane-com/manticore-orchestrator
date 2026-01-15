import { useTheme } from '../contexts/ThemeContext';
import manticoreLogoLight from '../assets/manticore-light.svg';
import manticoreLogoDark from '../assets/manticore-dark.svg';

interface ManticoreLogoProps {
  className?: string;
}

export const ManticoreLogo = ({ className = '' }: ManticoreLogoProps) => {
  const { effectiveTheme } = useTheme();
  const logo = effectiveTheme === 'dark' ? manticoreLogoDark : manticoreLogoLight;

  return (
    <div className={`inline-flex items-center ${className}`}>
      <img src={logo} alt="Manticore Orchestrator" className="h-14 w-auto" />
    </div>
  );
};
